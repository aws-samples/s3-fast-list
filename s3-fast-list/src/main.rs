mod error;
mod core;
mod data_map;
mod tasks_s3;
mod utils;
mod stats;
mod mon;
use std::io::BufRead;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use clap::{Parser, Subcommand};
use chrono::{Local, SecondsFormat};
use log::info;
use core::MB;
use core::RunMode;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,

    /// prefix to start with
    #[arg(short, long, default_value = "/", global=true)]
    prefix: String,

    /// worker threads for runtime
    #[arg(short, long, default_value_t = 10, global=true)]
    threads: usize,

    /// max concurrency tasks for list operation
    #[arg(short, long, default_value_t = 100, global=true)]
    concurrency: usize,

    /// input key space hints file [default: {region}_{bucket}_ks_hints.input]
    #[arg(short, long, global=true)]
    ks_file: Option<String>,

    /// object filter expresion
    #[arg(short, long, global=true)]
    filter: Option<String>,

    /// log to file [default: fastlist_{datetime}.log]
    #[arg(short, long, global=true)]
    log: bool,
}

#[derive(Subcommand)]
enum Commands {

    /// fast list and export results
    List {
        /// source aws region
        #[arg(long)]
        region: String,

        /// source bucket to list
        #[arg(long)]
        bucket: String,
    },

    /// bi-dir fast list and diff results
    Diff {
        /// source aws region
        #[arg(long)]
        region: String,

        /// source bucket to list
        #[arg(long)]
        bucket: String,

        /// target aws region
        #[arg(long)]
        target_region: String,

        /// target bucket to list
        #[arg(long)]
        target_bucket: String,
    },
}

fn main() {

    let cli = Cli::parse();
    let opt_mode;
    let opt_region;
    let opt_bucket;
    let opt_target_region;
    let opt_target_bucket;
    let opt_prefix = if cli.prefix == "/" { "".to_string() } else { cli.prefix };
    let opt_threads = cli.threads;
    let opt_concurrency = cli.concurrency;
    let opt_filter = cli.filter;

    // baseline count for all main tasks
    // data map task and mon task
    let mut g_tasks_count = 2;

    match &cli.cmd {
        Commands::List { region, bucket } => {
            opt_mode = RunMode::List;
            opt_region = region;
            opt_bucket = bucket;
            opt_target_region = None;
            opt_target_bucket = None;
            g_tasks_count += 1;
        },
        Commands::Diff { region, bucket, target_region, target_bucket } => {
            opt_mode = RunMode::BiDir;
            opt_region = region;
            opt_bucket = bucket;
            opt_target_region = Some(target_region);
            opt_target_bucket = Some(target_bucket);
            g_tasks_count += 2;
        },
    }

    // setup loglevel and log file
    let opt_log = cli.log;
    let package_name = env!("CARGO_PKG_NAME").replace("-", "_");
    let loglevel_s = format!("{}=info", package_name);
    let loglevel = std::env::var("RUST_LOG").unwrap_or(loglevel_s);
    if opt_log {
        let logfile_s = format!("fastlist_{}.log", Local::now().format("%Y%m%d%H%M%S"));
        let logfile = std::fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .append(true)
                                .open(&logfile_s)
                                .expect("unable to open log file");
        env_logger::Builder::new()
            .parse_filters(&loglevel)
            .target(env_logger::Target::Pipe(Box::new(logfile)))
            .init();
    } else {
        env_logger::Builder::new()
            .parse_filters(&loglevel)
            .init();
    }

    // gen dt string
    let dt_str = Local::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    // prepare ks hints list
    let mut ks_list: Vec::<String> = Vec::new();

    // check ks hints from cli input
    let opt_ks_file = cli.ks_file;
    let ks_filename = if let Some(f) = opt_ks_file {
        f.to_string()
    } else {
        // default ks hints input filename
        format!("{opt_region}_{opt_bucket}_ks_hints.input")
    };

    // load ks hints if exists
    if let Ok(f) = std::fs::File::open(&ks_filename) {
        let lines = std::io::BufReader::with_capacity(50*MB, f).lines();
        ks_list = lines.map(|l| l.unwrap()).collect();
    }

    // sort input lexicographically
    ks_list.sort();
    // dedup
    ks_list.dedup();
    let ks_list_len = ks_list.len();

    let ks_hints = data_map::KeySpaceHints::new_from(&ks_list);
    let ks_hints_pairs_len = ks_hints.len();

    info!("fast list tools v{} starting:", env!("CARGO_PKG_VERSION"));
    info!("  - mode {:?}, threads {}, concurrent tasks {}", opt_mode, opt_threads, opt_concurrency);
    info!("  - start prefix {}", opt_prefix);
    if opt_filter.is_some() {
        info!("  - filter \"{}\"", opt_filter.as_ref().unwrap());
    }
    if ks_list_len == 0 {
        info!("  - NO ks hints found");
    } else {
        info!("  - loaded {} prefix from input file {}, assembly into {} of ks hints pairs", ks_list_len, ks_filename, ks_hints_pairs_len);
    }

    let quit = Arc::new(AtomicBool::new(false));
    let q = quit.clone();
    ctrlc::set_handler(move || {
        q.store(true, Ordering::SeqCst);
    }).expect("failed to setting ctrl-c signal handler");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(opt_threads)
        .build()
        .unwrap();

    rt.block_on(async {
        let g_state = core::GlobalState::new(quit, g_tasks_count, 0);
        let mut set = tokio::task::JoinSet::new();

        let (data_map_channel, data_map_channel_rx) = tokio::sync::mpsc::unbounded_channel();

        // init left task
        let prefix = opt_prefix.clone();
        let dir = if opt_mode == RunMode::BiDir {
            core::S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE
        } else {
            core::S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE
        };
        let task_ctx = core::S3TaskContext::new(opt_region, opt_bucket,
            data_map_channel.clone(), dir, g_state.clone()
        );
        set.spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                tasks_s3::flat_list_main_task(&task_ctx, &prefix, opt_concurrency, ks_hints).await
            })
        });

        // init right task if bidir mode
        if opt_mode == RunMode::BiDir {
            let prefix = opt_prefix.clone();
            let task_ctx = core::S3TaskContext::new(opt_target_region.as_ref().unwrap(), opt_target_bucket.as_ref().unwrap(),
                data_map_channel, core::S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE, g_state.clone()
            );
            let ks_hints = data_map::KeySpaceHints::new_from(&ks_list);
            set.spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    tasks_s3::flat_list_main_task(&task_ctx, &prefix, opt_concurrency, ks_hints).await
                })
            });
        }

        // init data map task
        let data_map_ctx = core::DataMapContext::new(data_map_channel_rx, g_state.clone(), opt_filter, opt_mode.clone());
        let filename_ks = format!("{}_{}_{}.ks", opt_region, opt_bucket, dt_str);
        let filename_output = if opt_mode == RunMode::List {
            format!("{}_{}_{}.parquet", opt_region, opt_bucket, dt_str)
        } else {
            format!("{}_{}_{}_{}_{}.parquet", opt_region, opt_bucket,
                opt_target_region.as_ref().unwrap(), opt_target_bucket.as_ref().unwrap(), dt_str)
        };
        set.spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                data_map::data_map_task(data_map_ctx, filename_ks, filename_output).await
            })
        });

        // init mon task
        let mon_ctx = core::MonContext::new(g_state.clone());
        set.spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                mon::mon_task(mon_ctx).await
            })
        });

        while let Some(_) = set.join_next().await {
        }
        info!("All Tasks quit");
    });

    rt.shutdown_background();
}
