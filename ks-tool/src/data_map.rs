use std::time::Duration;
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use indicatif::{ProgressBar, style::ProgressStyle};

pub(crate) struct PrefixMap {
    inner: HashMap<String, usize>,
    count: usize,
}

impl PrefixMap {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            count: 0,
        }
    }

    pub fn get_count(&self) -> usize {
        self.count
    }

    pub fn inc_count(&mut self) {
        self.count += 1;
    }

    pub fn insert(&mut self, key: &str) {

        let prefix = key.rsplit_once('/')
                        .map_or("/".to_string(), |(p, _)| p.to_string());
        if let Some(objects_count) = self.inner.get_mut(&prefix) {
            *objects_count += 1;
            return;
        }
        self.inner.insert(prefix, 1);
        self.inc_count();
    }

    pub async fn dump_ks(&self, filename: &str, with_bar: bool) -> tokio::io::Result<(usize, usize)> {
        let buffer_size = 10 * 1024 * 1024;
        let inner = tokio::fs::File::create(filename).await?;
        let mut writer = tokio::io::BufWriter::with_capacity(buffer_size, inner);

        let mut object_count = 0;

        // btree to sort key in lex
        let mut btree = std::collections::BTreeMap::<&str, usize>::new();

        let bar = if with_bar {
            let bar = ProgressBar::new(self.inner.len() as u64);
            bar.enable_steady_tick(Duration::from_millis(200));
            bar.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {percent}% {msg}")
                    .unwrap()
                    .progress_chars("##-")
            );
            bar.set_message("Sorting prefix map ...");
            Some(bar)
        } else {
            None
        };

        for (prefix, count) in self.inner.iter() {
            btree.insert(prefix, *count);
            if let Some(ref b) = bar {
                b.inc(1);
            }
        }
        if let Some(ref b) = bar {
            b.finish_with_message("Sorting prefix map ... Done");
        }

        let bar = if with_bar {
            let bar = ProgressBar::new(self.inner.len() as u64);
            bar.enable_steady_tick(Duration::from_millis(200));
            bar.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {percent}% {msg}")
                    .unwrap()
                    .progress_chars("##-")
            );
            bar.set_message("Exporting prefix map ...");
            Some(bar)
        } else {
            None
        };

        // output key, count in sorted order
        for (prefix, count) in btree.iter() {
            let line = format!("\"{}\",\"{}\"\n", prefix, count);
            writer.write(line.as_bytes()).await?;
            object_count += count;
            if let Some(ref b) = bar {
                b.inc(1);
            }
        }

        writer.flush().await?;
        if let Some(ref b) = bar {
            b.finish_with_message("Exporting prefix map ... Done");
        }
        Ok((self.get_count(), object_count))
    }
}
