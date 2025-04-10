use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, Mutex};
use log::{info, warn, debug};
use crate::utils;
use crate::core;
use crate::core::MB;
use crate::core::{DataMapContext, ObjectKey, ObjectPrefix, ObjectName, ObjectProps, MatchResult};

const OUTPUT_DIR_FLAG_EQUAL: u8 = 0;
const OUTPUT_DIR_FLAG_PLUS: u8 = 1;
const OUTPUT_DIR_FLAG_MINUS: u8 = 2;
const OUTPUT_DIR_FLAG_ASTRISK: u8 = 3;

struct PrefixMap {
    inner: Arc<RwLock<HashMap<ObjectPrefix, ObjectMap>>>,
    count: Arc<AtomicUsize>,
}

impl PrefixMap {
    fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn inc_count(&self) {
        let _ = self.count.fetch_add(1, Ordering::SeqCst);
    }

    async fn get_object_hash(&self, prefix: &str) -> ObjectMap {

        let hash = self.inner.read().await;
        if let Some(obj_hash) = hash.get(prefix) {
            return obj_hash.clone();
        }
        drop(hash);

        let mut hash = self.inner.write().await;

        if let Some(obj_hash) = hash.get(prefix) {
            return obj_hash.clone();
        }
        let new = ObjectMap::new();
        hash.insert(prefix.to_string(), new.clone());
        self.inc_count();
        new
    }

    // return:
    //   - (prefix count, object count)
    fn get_stats(&self) -> (usize, usize) {
        let obj_count = tokio::task::block_in_place(move || {
            let fut = tokio::runtime::Handle::current().block_on(async move {

                let mut total = 0;
                let lock = self.inner.read().await;
                for o in lock.iter() {
                    total += o.1.get_count();
                }
                total
            });
            fut
        });
        let prefix_count = self.get_count();
        (prefix_count, obj_count)
    }

    // sync file op in async fn
    pub async fn dump_ks(&self, filename: &str) -> tokio::io::Result<()> {

        let buffer_size = 10 * MB;
        let inner = tokio::fs::File::create(filename).await?;
        let mut writer = tokio::io::BufWriter::with_capacity(buffer_size, inner);

        // btree to sort key in lex
        let mut btree = std::collections::BTreeMap::<ObjectPrefix, usize>::new();

        let hash = self.inner.read().await;
        for (prefix, obj_map) in hash.iter() {
            btree.insert(prefix.to_owned(), obj_map.get_count());
        }
        drop(hash);

        // output key, count in sorted order
        for (prefix, count) in btree.iter() {
            let line = format!("\"{}\",\"{}\"\n", prefix, count);
            writer.write(line.as_bytes()).await?;
        }

        writer.flush().await?;
        Ok(())
    }

    // sync file op in async fn
    pub async fn dump(&self, output_file: &str, include_equal: bool) -> tokio::io::Result<()> {

        let buffer_size = 100 * MB;
        let f = tokio::fs::File::create(output_file).await?;
        let writer = tokio::io::BufWriter::with_capacity(buffer_size, f);
        let mut parquet = utils::AsyncParquetOutput::new(writer);

        let hash = self.inner.read().await;

        for (prefix, obj_map) in hash.iter() {

            let mut plus = Vec::new();
            let mut minus = Vec::new();
            let mut astrisk = Vec::new();
            let mut equal = Vec::new();

            let map = obj_map.inner.lock().await;
            for (name, props) in map.iter() {
                match props.final_status_check() {
                    MatchResult::Plus => {
                        let key = ObjectKey::encode(prefix, &name);
                        plus.push((key, props.to_owned()));
                    },
                    MatchResult::Minus => {
                        let key = ObjectKey::encode(prefix, &name);
                        minus.push((key, props.to_owned()));
                    },
                    MatchResult::Astrisk => {
                        let key = ObjectKey::encode(prefix, &name);
                        astrisk.push((key, props.to_owned()));
                    },
                    MatchResult::Equal => {
                        if include_equal {
                            let key = ObjectKey::encode(prefix, &name);
                            equal.push((key, props.to_owned()));
                        }
                    },
                    MatchResult::Ignore => {
                        // object is filtered out
                    },
                    result @ _ => {
                        panic!("{:?} should not occurs here", result);
                    }
                }
            }
            drop(map);

            let _ = parquet.write(plus, OUTPUT_DIR_FLAG_PLUS).await;
            let _ = parquet.write(minus, OUTPUT_DIR_FLAG_MINUS).await;
            let _ = parquet.write(astrisk, OUTPUT_DIR_FLAG_ASTRISK).await;
            if include_equal {
                let _ = parquet.write(equal, OUTPUT_DIR_FLAG_EQUAL).await;
            }
        }
        drop(hash);

        let _ = parquet.close().await;

        Ok(())
    }
}

impl std::fmt::Display for PrefixMap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (prefix, object) = self.get_stats();
        write!(f, "prefix count {}, object count {}", prefix, object)
    }
}

#[derive(Clone)]
pub struct ObjectMap {
    inner: Arc<Mutex<HashMap<ObjectName, ObjectProps>>>,
    count: Arc<AtomicUsize>,
}

impl ObjectMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn inc_count(&self) {
        let _ = self.count.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    pub fn dec_count(&self) {
        let _ = self.count.fetch_sub(1, Ordering::SeqCst);
    }

    /*
     * return:
     *   - (vec for mtime check, vec for astrisk)
     */
    pub async fn bulk_insert(&self, prefix: &str, items: Vec<(ObjectName, ObjectProps)>) -> Vec<ObjectKey> {

        let mut astrisk = Vec::new();
        let _prefix = prefix.to_string();
        let mut hash = self.inner.lock().await;

        for item in items {
            let name = item.0;
            let props = item.1;
            if let Some(exist_props) = hash.get_mut(&name) {
                // compare and trigger next flow
                match exist_props.r#match(&props) {
                    MatchResult::Astrisk => {
                        // collect the confirmed astrisk one
                        let key = ObjectKey::encode(&_prefix, &name);
                        astrisk.push(key);
                    },
                    MatchResult::Dup => {
                        // if this is a duplicated props, just ignore it
                    },
                    MatchResult::Equal => {
                    },
                    MatchResult::Ignore => {
                    },
                    res @ _ => {
                        // we only care about the one need to do mtime check
                        // ignore other match result
                        // do nothing for time based equal here
                        debug!("match results {} {} {:?}", prefix, name.as_str(), res);
                    },
                }
            } else {
                hash.insert(name, props);
                self.inc_count();
            }
        }

        astrisk
    }
}

#[derive(Debug, Clone)]
pub struct KeySpacePair {
    index: usize,
    start: String,
    end: Option<String>,
}

impl KeySpacePair {
    pub fn new(index: usize, start: String, end: String) -> Self {

        let e = if end == "" { None } else { Some(end) };

        Self {
            index: index,
            start: start,
            end: e,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn to_task_input(&self) -> (&str, Option<&str>) {
        (&self.start, self.end.as_ref().map(|x| x.as_str()))
    }
}

pub struct KeySpaceHints {
    inner: VecDeque<KeySpacePair>,
    inflight: HashMap<usize, KeySpacePair>,
    done: Vec<KeySpacePair>,
}

impl KeySpaceHints {
    pub fn new_from(hints: &Vec<String>) -> Self {

        let mut v = VecDeque::new();

        let mut index = 0;
        let last = "".to_string();
        // start from top level
        let mut start = "".to_string();
        for key in hints {
            v.push_back(KeySpacePair::new(index, start.clone(), key.to_string()));
            start = key.to_string();
            index += 1;
        }
        // push last
        v.push_back(KeySpacePair::new(index, start, last));

        Self {
            inner: v,
            inflight: HashMap::new(),
            done: Vec::new(),
        }
    }

    pub fn next(&mut self) -> Option<KeySpacePair> {
        if let Some(pair) = self.inner.pop_front() {
            self.inflight.insert(pair.index(), pair.clone());
            return Some(pair);
        }
        None
    }

    pub fn finish(&mut self, index: usize) {
        if let Some(pair) = self.inflight.remove(&index) {
            self.done.push(pair);
            return;
        }
        panic!("index {} not in inflight list", index);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

async fn do_dump(map: &PrefixMap, filename_ks: &str, filename_output: &str) {

    let include_eq = false;
    info!("Data Map Task - final map stats {}", map);
    info!("Data Map Task - dumping all object keys to {}, include_eq: {}", filename_output, include_eq);
    let _ = map.dump(&filename_output, include_eq).await;
    info!("Data Map Task - dumping ks to {}", filename_ks);
    let _ = map.dump_ks(&filename_ks).await;
    info!("Data Map Task - quit");

    return ();
}

pub async fn data_map_task(mut ctx: DataMapContext, filename_ks: String, filename_output: String) -> () {

    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Data Map Task - started");

    let map = PrefixMap::new();
    let mut has_more_in_queue;

    let mut last_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    loop {
        match ctx.data_map_channel.try_recv() {
            Ok(hash) => {
                for (prefix, items) in hash.into_iter() {
                    let object_hash = map.get_object_hash(&prefix).await;
                    let _ = object_hash.bulk_insert(&prefix, items).await;
                }
                has_more_in_queue = true;
            },
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                has_more_in_queue = false;
            },
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                warn!("Data Map Task - consumer recv channel disconnected in data map task");
                break;
            }
        }

        if ctx.is_quit() {
            info!("Data Map Task - force quit, dump file *MAY INCONSISTENT*");
            do_dump(&map, &filename_ks, &filename_output).await;
            ctx.complete();
            return ();
        } else if !ctx.all_list_tasks_is_running() && !has_more_in_queue {
            do_dump(&map, &filename_ks, &filename_output).await;
            ctx.complete();
            ctx.quit();
            return ();
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            info!("Data Map Task - {}", map);
            tokio::task::yield_now().await;
            last_ts = now;
        }
    }
    ()
}
