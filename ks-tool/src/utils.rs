use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io::Cursor;
use std::collections::BTreeMap;
use tokio::fs::File;
use tokio::time::Duration;
use tokio::io::{BufReader, BufWriter, AsyncReadExt, AsyncWriteExt};
use csv::ReaderBuilder;
use serde::Deserialize;
use indicatif::ProgressBar;
use indicatif::style::ProgressStyle;

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

pub(crate) async fn handle_ks_input(quit: Arc<AtomicBool>, input: &str, splits: usize, output: &str) -> Result<(), tokio::io::Error> {

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
        if quit.load(Ordering::SeqCst) {
            bar.finish_with_message("Abort");
            return Ok(());
        }
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
        if quit.load(Ordering::SeqCst) {
            bar.finish_with_message("Abort");
            return Ok(());
        }
    }

    writer.flush().await?;
    bar.finish_with_message(".. Done");

    Ok(())
}

pub(crate) async fn handle_inventory_input(quit: Arc<AtomicBool>, input: &str, count: usize, output: &str) -> Result<(), tokio::io::Error> {
    let _ = quit;
    let _ = input;
    let _ = count;
    let _ = output;
    Ok(())
}
