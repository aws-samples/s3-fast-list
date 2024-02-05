use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::OnceLock;
use std::collections::HashMap;
use tokio::sync::Barrier;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use rhai::{Engine, AST, Scope};
use rhai::serde::to_dynamic;
use serde::{Deserialize, Serialize};
use log::{warn, error};
use crate::stats::HttpStatusCodeTracker;

#[allow(dead_code)]
pub(crate) const KB: usize = 1024;
pub(crate) const MB: usize = 1_048_576;
#[allow(dead_code)]
pub(crate) const GB: usize = 1_073_741_824;

pub(crate) const DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS: u64 = 1;

pub(crate) const DEFAULT_S3_CLIENT_TIMEOUT: u64 = 5;

pub(crate) const S3_TASK_CONTEXT_DIR_LEFT: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub(crate) const S3_TASK_CONTEXT_DIR_RIGHT: u8 = OBJECT_PROPS_FLAG_DIR_RIGHT;
pub(crate) const S3_TASK_CONTEXT_DIR_LEFT_LIST_MODE: u8 = OBJECT_PROPS_FLAG_DIR_LEFT;
pub(crate) const S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE: u8 = OBJECT_PROPS_FLAG_DIR_LEFT | OBJECT_PROPS_FLAG_DIFF_MODE;
pub(crate) const S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE: u8 = OBJECT_PROPS_FLAG_DIR_RIGHT | OBJECT_PROPS_FLAG_DIFF_MODE;

const S3_CLIENT_MAX_ATTEMPTS: u32 = 10;
const S3_CLIENT_INITIAL_BACKOFF: u64 = 30;
const S3_CLIENT_CONNECT_TIMEOUT: u64 = 60;

const OBJECT_PROPS_FLAG_S3_GP_BUCKET: u8 = 0b1;    // general purpose bucket
const OBJECT_PROPS_FLAG_S3_DIR_BUCKET: u8 = 0b10;   // directory bucket

const OBJECT_PROPS_FLAG_DIR_LEFT: u8 = 0b1000_0000;
const OBJECT_PROPS_FLAG_DIR_RIGHT: u8 = 0b0100_0000;
const OBJECT_PROPS_FLAG_DIR_BOTH: u8 = 0b1100_0000;
const OBJECT_PROPS_FLAG_DIFF_MODE: u8 = 0b0010_0000;

const OBJECT_PROPS_STATUS_OPEN: u8 = 0xFF;
const OBJECT_PROPS_STATUS_MATCH: u8 = 0x0;
const OBJECT_PROPS_STATUS_SIZE_NOT_MATCH: u8 = 1;
const OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL: u8 = 2;
const OBJECT_PROPS_STATUS_ETAG_NOT_MATCH: u8 = 3;
const OBJECT_PROPS_STATUS_FILTER_OUT: u8 = 4;

const OBJECT_FILTER_ALLOWED_VARIABLE: [&str; 2] = ["SOURCE", "TARGET"];
const OBJECT_FILTER_ALLOWED_PROPERTY: [&str; 2] = ["size", "last_modified"];
pub(crate) static OBJECT_FILTER: OnceLock<ObjectFilter> = OnceLock::new();

#[derive(Debug)]
pub(crate) struct ObjectFilter {
    engine: Engine,
    ast: AST,
}

impl ObjectFilter {
    // check any invalidation of input filter experssion
    fn check_expr(&self, mode: RunMode) -> bool {

        let mut check_failed = false;
        /*
         * since rhai only check object map existence when the expression is executed to specific condition,
         * go through AST to check all variable's props to ensure only allowed object props is used.
        */
        self.ast.walk(&mut|nodes| {
            let mut cont = true;
            for node in nodes {
                match node {
                    rhai::ASTNode::Expr(expr) => {
                        match expr {
                            // Property(Box<((getter, hash), (setter, hash), prop)>, position)
                            rhai::Expr::Property(props, _) => {
                                let prop = props.2.as_str();
                                if !OBJECT_FILTER_ALLOWED_PROPERTY.contains(&prop) {
                                    error!("object property \"{prop}\" not allowed");
                                    cont = false;
                                    check_failed = true;
                                }
                            },
                            rhai::Expr::Variable(names, _, _) => {
                                let name = names.3.as_str();
                                if !OBJECT_FILTER_ALLOWED_VARIABLE.contains(&name) {
                                    error!("variable \"{name}\" not allowed");
                                    cont = false;
                                    check_failed = true;
                                }
                            },
                            _ => {},
                        }
                    }
                    _ => {},
                }
            }
            cont
        });

        if check_failed {
            return false;
        }

        // then we build fake ObjectProps to test AST to see
        // any other invalidation exist in expression
        let mut scope = Scope::new();
        let source = to_dynamic(ObjectProps::default()).unwrap();
        scope.push_constant_dynamic("SOURCE", source.into_read_only());

        if mode == RunMode::BiDir {
            let target = to_dynamic(ObjectProps::default()).unwrap();
            scope.push_constant_dynamic("TARGET", target.into_read_only());
        }

        match self.engine.eval_ast_with_scope::<bool>(&mut scope, &self.ast) {
            Ok(_) => {
                return true;
            },
            Err(e) => {
                error!("validation filter expression failed: {e:?}");
                return false;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RunMode {
    List,
    BiDir,
}

#[repr(transparent)]
#[derive(Debug)]
pub struct ObjectKey(String);
pub type ObjectName = String;
pub type ObjectPrefix = String;

impl ObjectKey {
    // decode object key into (prefix, name) pair
    // use "/" as special placeholder if prefix is ""
    pub fn decode(&self) -> (ObjectPrefix, ObjectName) {
        self.0.rsplit_once('/')
            .map_or(
                /* replace top level prefix to "/" */
                ("/".to_owned(), self.0.to_owned()),
                |(p, n)| (p.to_owned(), n.to_owned())
            )
    }

    pub fn encode(prefix: &ObjectPrefix, name: &ObjectName) -> Self {
        if prefix == "/" {
            return Self (
                name.to_string()
            );
        }
        Self (
            vec![prefix.as_str(), name.as_str()].join("/")
        )
    }

    #[allow(dead_code)]
    pub fn starts_with(&self, end: &str) -> bool {
        self.0.starts_with(end)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for ObjectKey {
    fn from(item: &str) -> Self {
        Self {
            0: item.to_string()
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MatchResult {
    Equal = 0,
    Plus = 1,
    Minus = 2,
    Astrisk = 3,
    Dup = 4,
    Ignore = 5,
}

#[repr(align(8))]
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ObjectProps {
    #[serde(skip)]
    flags: u8,
    #[serde(skip)]
    status: u8,
    #[allow(dead_code)]
    #[serde(skip)]
    pad: u16,
    #[serde(skip)]
    etag_parts: u32,
    last_modified: u64,
    size: u64,
    #[serde(skip)]
    etag_md5: [u8; 16],
}

impl ObjectProps {

    pub fn set_dir(&mut self, dir: u8) {
        self.flags |= dir;
    }

    pub fn is_diff_mode(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIFF_MODE) == OBJECT_PROPS_FLAG_DIFF_MODE
    }

    #[allow(dead_code)]
    pub fn set_bucket_type_gp(&mut self) {
        self.flags |= OBJECT_PROPS_FLAG_S3_GP_BUCKET;
    }

    #[allow(dead_code)]
    pub fn set_bucket_type_dir(&mut self) {
        self.flags |= OBJECT_PROPS_FLAG_S3_DIR_BUCKET;
    }

    pub fn is_left(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIR_LEFT) == OBJECT_PROPS_FLAG_DIR_LEFT
    }

    pub fn is_right(&self) -> bool {
        (self.flags & OBJECT_PROPS_FLAG_DIR_RIGHT) == OBJECT_PROPS_FLAG_DIR_RIGHT
    }

    pub fn is_etag_avail(&self) -> bool {
        let (prefix, aligned, suffix) = unsafe { self.etag_md5.align_to::<u128>() };
        return prefix.iter().all(|&x| x == 0)
            && suffix.iter().all(|&x| x == 0)
            && aligned.iter().all(|&x| x == 0)
            && self.etag_parts == 0;
    }

    pub fn etag(&self) -> ([u8; 16], u32) {
        (self.etag_md5, self.etag_parts)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn last_modified(&self) -> u64 {
        self.last_modified
    }

    pub fn etag_string(&self) -> String {

        let s = if self.etag_parts == 0 {
            format!("{}", hex::encode(self.etag_md5))
        } else {
            format!("{}-{}", hex::encode(self.etag_md5), self.etag_parts)
        };
        s
    }

    // used only in final dump stage
    pub fn final_status_check(&self) -> MatchResult {

        // if we are in list mode and we have filter set, do apply here
        if !self.is_diff_mode() {
            if let Some(filter) = OBJECT_FILTER.get() {
                let mut scope = Scope::new();
                let source = to_dynamic(self).unwrap();
                scope.push_constant_dynamic("SOURCE", source.into_read_only());
                match filter.engine.eval_ast_with_scope(&mut scope, &filter.ast) {
                    Ok(false) => {
                        return MatchResult::Ignore;
                    },
                    Ok(true) => {
                        // if filter condition matched, just continue
                    },
                    e @ _ => {
                        // if apply filter error, just continue
                        warn!("failed to apply filter for {self:?}, error: {e:?}");
                    }
                }
            }
        }

        if self.status == OBJECT_PROPS_STATUS_FILTER_OUT {
            return MatchResult::Ignore;
        }

        if self.status == OBJECT_PROPS_STATUS_SIZE_NOT_MATCH ||
            self.status == OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL ||
            self.status == OBJECT_PROPS_STATUS_ETAG_NOT_MATCH {

            return MatchResult::Astrisk;
        }

        // merge match and time based match into equal
        if self.status == OBJECT_PROPS_STATUS_MATCH {
            return MatchResult::Equal;
        }

        if self.status == OBJECT_PROPS_STATUS_OPEN {

            // if status is in OPEN, only one side set the flag
            assert!((self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) != OBJECT_PROPS_FLAG_DIR_BOTH);

            if self.is_left() {
                return MatchResult::Plus;
            } else if self.is_right() {
                return MatchResult::Minus;
            } else {
                // flag is zero ?
                panic!("object props flags {} status {}, this should not happen !", self.flags, self.status);
            }
        }

        panic!("object props flags {} status {}, why ?", self.flags, self.status);
    }

    pub fn r#match(&mut self, other: &ObjectProps) -> MatchResult {

        /*
         *  NOTE:
         *    because underlying client on both side could get error and retry,
         *    same object's props on either side could be retrived multiple times,
         *    if we got same object's props at same side, just override it with the latest one.
         */

        /*
         * if we already set both side flags
         * obviously this is a duplicated props
         * just ignore this
         */
        if (self.flags & OBJECT_PROPS_FLAG_DIR_BOTH) == OBJECT_PROPS_FLAG_DIR_BOTH {
            return MatchResult::Dup;
        }

        /* one side duplicate case */
        if (self.is_right() && other.is_right()) || (self.is_left() && other.is_left()) {
            *self = other.clone();
            return MatchResult::Dup;
        }

        let (left, right): (&ObjectProps, &ObjectProps) = if self.is_left() {
                (self, other)
            } else {
                assert!(self.is_right());
                (other, self)
            };

        // apply filter if we have
        if let Some(filter) = OBJECT_FILTER.get() {
            // should be in diff mode
            assert!(left.is_diff_mode() && right.is_diff_mode());
            let mut scope = Scope::new();
            let source = to_dynamic(left).unwrap();
            let target = to_dynamic(right).unwrap();
            scope.push_constant_dynamic("SOURCE", source.into_read_only());
            scope.push_constant_dynamic("TARGET", target.into_read_only());
            match filter.engine.eval_ast_with_scope(&mut scope, &filter.ast) {
                Ok(false) => {
                    *self = left.clone();
                    self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
                    self.status = OBJECT_PROPS_STATUS_FILTER_OUT;
                    return MatchResult::Ignore;
                },
                Ok(true) => {
                    // if filter condition matched, just continue
                },
                e @ _ => {
                    // if apply filter error, just continue
                    warn!("failed to apply filter for {left:?} and {right:?}, error: {e:?}");
                }
            }
        }

        // if size not match, override the entry with left's data
        if left.size != right.size {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_SIZE_NOT_MATCH;
            return MatchResult::Astrisk;
        }

        // if size eq but missing md5 value on either side
        // mark this *
        if left.is_etag_avail() || right.is_etag_avail() {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_ETAG_NOT_AVAIL;
            return MatchResult::Astrisk;
        }

        // if we have md5 value on both side
        if left.etag() != right.etag() {
            *self = left.clone();
            self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
            self.status = OBJECT_PROPS_STATUS_ETAG_NOT_MATCH;
            return MatchResult::Astrisk;
        }

        *self = left.clone();
        self.flags |= OBJECT_PROPS_FLAG_DIR_BOTH;
        self.status = OBJECT_PROPS_STATUS_MATCH;
        return MatchResult::Equal;
    }
}

impl From<&aws_sdk_s3::types::Object> for ObjectProps {
    fn from(item: &aws_sdk_s3::types::Object) -> Self {
        let mut md5 = [0u8; 16];
        let (etag_md5, etag_parts) = item.e_tag()
                                        .map_or((md5, 0), |x| {
                                            if x.len() == 34 {
                                                // etag string in decoded like:
                                                // "d41d8cd98f00b204e9800998ecf8427e"
                                                if let Ok(_) = hex::decode_to_slice(&x[1..33], &mut md5) {
                                                    return (md5, 0);
                                                }
                                            } else if x.len() >= 36 {
                                                //  md5 string like:
                                                // "c8af37b371ec442ad415feeb87d83246-186"
                                                // format check
                                                if x.chars().nth(33) != Some('-') {
                                                    panic!("unhandled etag format {}", x);
                                                }
                                                if let Ok(_) = hex::decode_to_slice(&x[1..33], &mut md5) {
                                                    if let Ok(parts) = &x[34..x.len()-1].parse::<usize>() {
                                                        return (md5, *parts as u32);
                                                    }
                                                }
                                            }
                                            panic!("unhandled etag format {}", x);
                                        });
        Self {
            flags: OBJECT_PROPS_FLAG_S3_GP_BUCKET,
            status: OBJECT_PROPS_STATUS_OPEN,
            pad: 0,
            etag_parts: etag_parts,
            last_modified: item
                .last_modified()
                .map_or(0, |x| x.secs() as u64),
            size: item
                .size()
                .map_or(0, |x| x as u64),
            etag_md5: etag_md5,
        }
    }
}

#[derive(Clone)]
struct TaskRendezvous {
    barrier: Arc<Barrier>,
    warmup_secs: u64,
}

impl TaskRendezvous {
    fn new(tasks_count: usize, warmup_secs: u64) -> Self {
        Self {
            barrier: Arc::new(Barrier::new(tasks_count)),
            warmup_secs: warmup_secs,
        }
    }

    #[inline]
    async fn wait(&self) {
        let _ = self.barrier.wait().await;
    }

    #[inline]
    fn get_warmup_secs(&self) -> u64 {
        self.warmup_secs
    }
}

const TASK_STATUS_BIT_LEFT: usize = 0x1;
const TASK_STATUS_BIT_RIGHT: usize = 0x2;
const TASK_STATUS_BIT_DATA_MAP: usize = 0x4;
const TASK_STATUS_BIT_MON: usize = 0x8;

#[derive(Clone)]
pub struct GlobalState {
    state: Arc<AtomicUsize>,
    quit: Arc<AtomicBool>,
    tracker: Arc<HttpStatusCodeTracker>,
    task_next_stream_timeout_count: Arc<AtomicUsize>,
    s3_client_timeout_count: Arc<AtomicUsize>,
    s3_client_generic_error_count: Arc<AtomicUsize>,
    task_rendez: TaskRendezvous,
}

impl GlobalState {
    pub fn new(quit: Arc<AtomicBool>, tasks_count: usize, warmup_secs: u64) -> Self {
        Self {
            state: Arc::new(AtomicUsize::new(0)),
            quit: quit,
            tracker: Arc::new(HttpStatusCodeTracker::new()),
            task_next_stream_timeout_count: Arc::new(AtomicUsize::new(0)),
            s3_client_timeout_count: Arc::new(AtomicUsize::new(0)),
            s3_client_generic_error_count: Arc::new(AtomicUsize::new(0)),
            task_rendez: TaskRendezvous::new(tasks_count, warmup_secs),
        }
    }

    pub async fn wait_to_start(&self) {
        self.task_rendez.wait().await;
    }

    #[allow(dead_code)]
    pub fn get_warmup_secs(&self) -> u64 {
        self.task_rendez.get_warmup_secs()
    }

    pub fn inc_task_next_stream_timeout(&self) {
        self.task_next_stream_timeout_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn read_task_next_stream_timeout(&self) -> usize {
        self.task_next_stream_timeout_count.load(Ordering::SeqCst)
    }

    pub fn inc_s3_client_timeout(&self) {
        self.s3_client_timeout_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn read_s3_client_timeout(&self) -> usize {
        self.s3_client_timeout_count.load(Ordering::SeqCst)
    }

    pub fn inc_s3_client_generic_error(&self) {
        self.s3_client_generic_error_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn read_s3_client_generic_error(&self) -> usize {
        self.s3_client_generic_error_count.load(Ordering::SeqCst)
    }

    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        Arc::clone(&self.tracker)
    }

    pub fn start(&self, mask: usize) {
        self.state.fetch_or(mask, Ordering::SeqCst);
    }

    pub fn complete(&self, mask: usize) {
        self.state.fetch_and(!mask, Ordering::SeqCst);
    }

    pub fn is_running(&self, mask: usize) -> bool {
        self.state.load(Ordering::SeqCst) & mask != 0
    }

    pub fn quit(&self) {
        self.quit.store(true, Ordering::SeqCst);
    }

    pub fn is_quit(&self) -> bool {
        self.quit.load(Ordering::SeqCst)
    }

    pub fn list_task_start(&self, dir: u8) {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                self.start(TASK_STATUS_BIT_LEFT);
            },
            S3_TASK_CONTEXT_DIR_RIGHT | S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE => {
                self.start(TASK_STATUS_BIT_RIGHT);
            },
            _ => {
                panic!("unkown dir {} for list task", dir);
            }
        }
    }

    pub fn list_task_complete(&self, dir: u8) {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                self.complete(TASK_STATUS_BIT_LEFT);
            },
            S3_TASK_CONTEXT_DIR_RIGHT | S3_TASK_CONTEXT_DIR_RIGHT_DIFF_MODE => {
                self.complete(TASK_STATUS_BIT_RIGHT);
            },
            _ => {
                panic!("unkown dir {} for list task", dir);
            }
        }
    }

    pub fn list_task_is_running(&self, dir: u8) -> bool {
        match dir {
            S3_TASK_CONTEXT_DIR_LEFT | S3_TASK_CONTEXT_DIR_LEFT_DIFF_MODE => {
                return self.is_running(TASK_STATUS_BIT_LEFT);
            },
            S3_TASK_CONTEXT_DIR_RIGHT => {
                return self.is_running(TASK_STATUS_BIT_RIGHT);
            },
            _ => {
                panic!("unkown dir {} for list task", dir);
            }
        }
    }

    pub fn all_list_tasks_is_running(&self) -> bool {
        self.is_running(TASK_STATUS_BIT_LEFT | TASK_STATUS_BIT_RIGHT)
    }

    pub fn data_map_task_start(&self) {
        self.start(TASK_STATUS_BIT_DATA_MAP);
    }

    pub fn data_map_task_complete(&self) {
        self.complete(TASK_STATUS_BIT_DATA_MAP);
    }

    pub fn data_map_task_is_running(&self) -> bool {
        self.is_running(TASK_STATUS_BIT_DATA_MAP)
    }

    pub fn mon_task_start(&self) {
        self.start(TASK_STATUS_BIT_MON);
    }

    pub fn mon_task_complete(&self) {
        self.complete(TASK_STATUS_BIT_MON);
    }
}

#[derive(Clone)]
pub(crate) struct S3TaskContext {
    pub s3_bucket_name: String,
    pub s3_client: aws_sdk_s3::Client,
    pub data_map_channel: UnboundedSender<HashMap<ObjectPrefix, Vec<(ObjectName, ObjectProps)>>>,
    pub dir: u8,
    pub g_state: GlobalState,
}

impl S3TaskContext {
    pub fn new(region: &str, bucket: &str, data_map_channel: UnboundedSender<HashMap<ObjectPrefix, Vec<(ObjectName, ObjectProps)>>>,
            dir: u8, g_state: GlobalState) -> Self {

        // create and config s3 client
        let loader = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new(region.to_owned()))
            .retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(S3_CLIENT_MAX_ATTEMPTS)
                    .with_initial_backoff(std::time::Duration::from_secs(S3_CLIENT_INITIAL_BACKOFF))
            )
            .timeout_config(
                aws_config::timeout::TimeoutConfigBuilder::new()
                    .connect_timeout(std::time::Duration::from_secs(S3_CLIENT_CONNECT_TIMEOUT))
                    .build()
            );

        let config = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move {
                    loader.load().await
                })
            });

        let s3_client = aws_sdk_s3::Client::new(&config);

        Self {
            s3_bucket_name: bucket.to_string(),
            s3_client: s3_client,
            data_map_channel: data_map_channel,
            dir: dir,
            g_state: g_state,
        }
    }

    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        self.g_state.get_tracker()
    }

    pub fn start(&self) {
        self.g_state.list_task_start(self.dir);
    }

    pub fn complete(&self) {
        self.g_state.list_task_complete(self.dir);
    }

    pub fn is_running(&self) -> bool {
        self.g_state.list_task_is_running(self.dir)
    }

    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }
}

pub(crate) struct DataMapContext {
    pub data_map_channel: UnboundedReceiver<HashMap<ObjectPrefix, Vec<(ObjectName, ObjectProps)>>>,
    pub g_state: GlobalState,
}

impl DataMapContext {
    pub fn new(data_map_channel: UnboundedReceiver<HashMap<ObjectPrefix, Vec<(ObjectName, ObjectProps)>>>, g_state: GlobalState, opt_filter: Option<String>, opt_mode: RunMode) -> Self {
        // init filter if we got from cli
        if let Some(expr) = opt_filter {
            let mut engine = Engine::new();
            engine.set_fail_on_invalid_map_property(true)
                .set_max_variables(2) // only SOURCE and TARGET
                .set_max_map_size(2); // only size and last_modified

            let ast = engine.compile_expression(expr);
            if ast.is_err() {
                error!("unable to compile object filter expression: {ast:?}");
                std::process::exit(1);
            }
            let filter = ObjectFilter {
                engine: engine,
                ast: ast.unwrap(),
            };
            let res = filter.check_expr(opt_mode);
            if res != true {
                std::process::exit(1);
            }
            let res = OBJECT_FILTER.set(filter);
            if res.is_err() {
                error!("unable to set value to global object filter: {res:?}");
                std::process::exit(1);
            }
        }

        Self {
            data_map_channel: data_map_channel,
            g_state: g_state,
        }
    }

    pub fn start(&self) {
        self.g_state.data_map_task_start();
    }

    pub fn complete(&self) {
        self.g_state.data_map_task_complete();
    }

    #[allow(dead_code)]
    pub fn is_running(&self) -> bool {
        self.g_state.data_map_task_is_running()
    }

    pub fn quit(&self) {
        self.g_state.quit()
    }

    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }

    pub fn all_list_tasks_is_running(&self) -> bool {
        self.g_state.all_list_tasks_is_running()
    }
}

pub(crate) struct MonContext {
    pub g_state: GlobalState,
}

impl MonContext {
    pub fn new(g_state: GlobalState) -> Self {
        Self {
            g_state: g_state,
        }
    }

    pub fn get_tracker(&self) -> Arc<HttpStatusCodeTracker> {
        self.g_state.get_tracker()
    }

    pub fn start(&self) {
        self.g_state.mon_task_start();
    }

    pub fn complete(&self) {
        self.g_state.mon_task_complete();
    }

    pub fn is_quit(&self) -> bool {
        self.g_state.is_quit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key() {
        let key: ObjectKey = ObjectKey::from("test.jpg");
        let (prefix, name) = key.decode();
        assert_eq!(prefix, "/");
        assert_eq!(name, "test.jpg");

        let key = ObjectKey::encode(&prefix, &name);
        assert_eq!(key.as_str(), "test.jpg");

        let key: ObjectKey = ObjectKey::from("a/b/c/test.jpg");
        let (prefix, name) = key.decode();
        assert_eq!(prefix, "a/b/c");
        assert_eq!(name, "test.jpg");

        let key = ObjectKey::encode(&prefix, &name);
        assert_eq!(key.as_str(), "a/b/c/test.jpg");
    }
}
