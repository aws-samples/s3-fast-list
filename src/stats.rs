use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct HttpStatusCodeTracker {
    map: RwLock<HashMap<u16, Arc<AtomicUsize>>>
}

impl HttpStatusCodeTracker {

    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new())
        }
    }

    pub async fn inc(&self, code: u16) {
        let lock = self.map.read().await;
        let val = lock.get(&code);
        if val.is_some() {
            let counter = Arc::clone(val.unwrap());
            // drop lock immediately as we get back atomic
            drop(lock);
            let _ = counter.fetch_add(1, Ordering::SeqCst);
            return;
        }
        drop(lock);

        // if http status code is not found in hash map,
        // insert a new counter with initial value 1
        let mut lock = self.map.write().await;
        if let Some(counter) = lock.get_mut(&code) {
            let _ = counter.fetch_add(1, Ordering::SeqCst);
            return;
        }
        lock.insert(code, Arc::new(AtomicUsize::new(1)));
    }

    #[allow(dead_code)]
    pub fn inc_in_place(&self, code: u16) {
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                self.inc(code).await;
            });
        });
    }

    // create a snapshot of all keys and values
    pub fn snapshot(&self) -> Vec<(u16, usize)> {

        let res = tokio::task::block_in_place(move || {
            let _res = tokio::runtime::Handle::current().block_on(async move {
                let lock = self.map.read().await;
                let vec = lock.iter().map(|(code, counter)|
                    (*code, counter.load(Ordering::SeqCst))
                ).collect();
                vec
            });
            _res
        });

        res
    }
}

impl std::fmt::Display for HttpStatusCodeTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut snap = self.snapshot();
        snap.sort_by_key(|v| v.0);
        let stats: Vec<String> = snap.iter()
            .map(|(code, count)| format!("[{} => {}]", code, count))
            .collect();
        write!(f, "{}", stats.join(", "))
    }
}
