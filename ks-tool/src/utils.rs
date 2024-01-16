use std::sync::Arc;
use std::io::Cursor;
use std::io::Read;
use std::collections::BTreeMap;
use tokio::fs::File;
use tokio::time::Duration;
use tokio::io::{BufReader, BufWriter, AsyncReadExt, AsyncWriteExt};
use tokio::io::{Error, ErrorKind};
use tokio::task::JoinSet;
use tokio::sync::Mutex;
use aws_sdk_s3::Client as S3Client;
use csv::ReaderBuilder;
use serde::Deserialize;
use indicatif::{ProgressBar, style::ProgressStyle};
use flate2::read::GzDecoder;
use s3_transfer_manager::manager::S3TransferManager;
use s3_manifest::inventory::InventoryManifest;
use crate::arn::Arn;
use crate::data_map::PrefixMap;

const TICK_STRINGS: [&str; 7] = [
    "▹▹▹▹▹",
    "▸▹▹▹▹",
    "▹▸▹▹▹",
    "▹▹▸▹▹",
    "▹▹▹▸▹",
    "▹▹▹▹▸",
    "▪▪▪▪▪",
];

#[derive(Debug, Deserialize)]
struct KsRow {
    prefix: String,
    objects: usize,
}

pub(crate) async fn handle_ks_input(input: &str, splits: usize, output: &str) -> Result<(), Error> {

    let mut map: BTreeMap<String, usize> = BTreeMap::new();
    let mut buf = Vec::new();
    let mut total_objects = 0;

    // loading file contents from input
    let file = File::open(input).await?;
    let mut reader = BufReader::with_capacity(100*1024*1024, file);

    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(Duration::from_millis(200));
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {spinner:.blue} {msg}")
            .unwrap()
            .tick_strings(&TICK_STRINGS)
    );
    let msg = format!("Loading file {} into memory", input);
    bar.set_message(msg.clone());

    reader.read_to_end(&mut buf).await?;

    bar.finish_with_message(msg + " .. Done");

    // building in memory btree map for prefix
    let cursor = Cursor::new(buf);
    let mut rd = ReaderBuilder::new()
            .has_headers(false)
            .double_quote(true)
            .from_reader(cursor);

    let iter = rd.deserialize();

    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(Duration::from_millis(200));
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {spinner:.blue} Building {len} prefix {msg}")
            .unwrap()
            .tick_strings(&TICK_STRINGS)
    );

    for res in iter {
        let row: KsRow = res.unwrap();
        total_objects += row.objects;
        map.insert(row.prefix, row.objects);
        bar.inc(1);
    }

    bar.finish_with_message(".. Done");

    // calculating objects count to find right prefix to split
    let avg_target = total_objects / splits;
    if avg_target == 0 {
        println!("avg objects for target splits is {avg_target}, not need to create ks hints");
        return Ok(());
    }

    let bar = ProgressBar::new(total_objects as u64);
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {spinner:.blue} Exporting {percent}% {msg}")
            .unwrap()
            .tick_strings(&TICK_STRINGS)
    );
    bar.set_message("exported");

    let file = File::create(output).await?;
    let mut writer = BufWriter::new(file);

    let mut cum_count = 0;
    let mut last_prefix = "";
    for (prefix, count) in map.iter() {
        cum_count += count;
        if cum_count > avg_target {
            writer.write(last_prefix.as_bytes()).await?;
            writer.write(b"\n").await?;
            bar.inc(cum_count as u64);
            cum_count = 0;
        }
        last_prefix = prefix;
    }

    writer.flush().await?;
    bar.finish_with_message(".. Done");

    Ok(())
}

pub(crate) async fn handle_inventory_input(input: &str, count: usize, output: &str) -> Result<(), tokio::io::Error> {
    let _ = input;
    let _ = count;
    let _ = output;
    Ok(())
}

// download all inventory to build prefix map and dump to ks
async fn build_and_dump_prefix_map(client: S3Client,
        dest_bucket: &str,
        mut keys: Vec<String>,
        output: &str,
        max_concurrency: usize)
    -> Result<(), Error>
{

    let prefix_map = Arc::new(Mutex::new(PrefixMap::new()));
    let mut set: JoinSet<Result<(), Error>> = JoinSet::new();
    let multibar = indicatif::MultiProgress::new();

    // count progress
    let total = keys.len();
    let mut started = 0;

    loop {
        if set.len() >= max_concurrency || (set.len() > 0 && keys.len() == 0) {
            // if reached max concurrency
            //   or
            // all keys consumed but still has task in progress
            // wait completion for one task
            let _ = set.join_next().await;
            continue;
        }

        // time to spawn a new task
        if let Some(key) = keys.pop() {
            let c = client.clone();
            let map = prefix_map.clone();
            let bucket = dest_bucket.to_string();
            let mb = multibar.clone();

            started += 1;

            set.spawn(async move {

                let uri = format!("s3://{}/{}", &bucket, key);

                // prepare bar
                let bar = ProgressBar::new(0);
                let bar = mb.add(bar);
                bar.set_style(
                    ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {binary_bytes_per_sec} {percent:>3}% {msg} {total_bytes:7}")
                        .unwrap()
                        .progress_chars("##-")
                );

                let filename = key.rsplit_once('/').unwrap().1.to_string();
                bar.set_message(format!("[{started}/{total}] {}", filename));
                let clone = bar.clone();
                let set = move |x| { clone.set_length(x as u64) };
                let clone = bar.clone();
                let update = move |x| { clone.inc(x as u64) };
                let clone = bar.clone();
                let finish = move | | { clone.finish() };

                let tm = S3TransferManager::new(c)
                    .with_update_progress(set, update, finish);

                let mut buf = Vec::new();
                tm.download(&uri, &mut buf).await?;

                let output = tokio::task::spawn_blocking(move || {
                    let mut output = Vec::new();
                    let mut gz = GzDecoder::new(&buf[..]);
                    let _ = gz.read_to_end(&mut output);
                    output
                }).await?;

                let mut rdr = ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(Cursor::new(output));

                let _ = tokio::task::spawn_blocking(move || {
                    let mut lock = map.blocking_lock();
                    for result in rdr.records() {
                        let record = result?;
                        let key = record.get(1).unwrap();
                        // key in s3 inventory CSV is URL-encoded
                        let decoded = urlencoding::decode(key).expect("UTF-8");
                        lock.insert(&decoded);
                    }
                    Ok::<(), Error>(())
                }).await?;
                mb.remove(&bar);
                Ok(())
            });
        }

        if set.len() == 0 && keys.len() == 0 {
            break;
        }
    }
    let lock = prefix_map.lock().await;
    let (prefix_count, object_count) = lock.dump_ks(output, true).await?;
    println!("{} generated with {} prefixes and {} objects", output, prefix_count, object_count);
    Ok(())
}

pub(crate) async fn inventory_to_ks(region: &str, manifest: &str, ks: Option<&String>, max_concurrency: usize) -> Result<(), Error> {
    let r = region.to_string();
    // build default client with region from params
    let config = aws_config::from_env()
        .region(aws_config::Region::new(r))
        .load()
        .await;
    let client = S3Client::new(&config);
    let tm = S3TransferManager::new(client.clone());

    // read in manifest
    let mut buf = Vec::new();
    tm.download(manifest, &mut buf).await?;
    let inventory: InventoryManifest = serde_json::from_slice(&buf).unwrap();
    if inventory.file_format != "CSV" {
        let msg = format!("ONLY CSV format inventory supported, but we got {}", inventory.file_format);
        return Err(Error::new(ErrorKind::InvalidData, msg));
    }

    let dest_arn = inventory.destination_bucket;
    let arn = Arn::parse(&dest_arn).expect("valid ARN");
    let dest_bucket = arn.resource_id()[0];
    let keys = inventory.files.into_iter().map(|f| f.key).collect();

    let output = if let Some(ks_output) = ks {
        ks_output.to_string()
    } else {
        format!("{}_{}.ks", region, inventory.source_bucket)
    };
    build_and_dump_prefix_map(client, dest_bucket, keys, &output, max_concurrency).await?;
    Ok(())
}
