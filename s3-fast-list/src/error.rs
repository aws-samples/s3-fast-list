use std::sync::Arc;
use crate::stats::HttpStatusCodeTracker;

pub const ERROR_S3_NEXT_STREAM_TIMEOUT: u8 = 0x1;
pub const ERROR_S3_CLIENT_GENERIC: u8 = 0x2;
pub const ERROR_S3_CLIENT_CONNECTION_TIMEOUT: u8 = 0x3;
pub const ERROR_S3_MISSING_REGION: u8 = 0x4;
pub const ERROR_S3_NO_BUCKET: u8 = 0x10;
pub const ERROR_S3_ACCESS_DENIED: u8 = 0x11;
pub const ERROR_S3_PERMANENT_REDIRECT: u8 = 0x12;
pub const ERROR_S3_UNKOWN: u8 = 0xff;

#[derive(Debug, Clone)]
pub struct FlatRuntimeError {
    errno: u8,
    errmsg: String,
    next_start: String,
    // http status code or 0 if we don't have
    http_status_code: u16,
}

impl FlatRuntimeError {
    pub fn new(errno: u8, errmsg: String, next_start: String) -> Self {
        Self {
            errno: errno,
            errmsg: errmsg,
            next_start: next_start,
            http_status_code: 0,
        }
    }

    pub fn next_start(&self) -> String {
        self.next_start.to_string()
    }

    pub fn continue_on_error(&self) -> bool {
        if self.errno < ERROR_S3_NO_BUCKET {
            return true;
        }
        false
    }

    pub fn with_http_status_code(self, code: u16) -> Self {
        let mut s = self;
        s.http_status_code = code;
        s
    }

    pub fn with_http_status_code_tracker(self, code: u16, tracker: Arc<HttpStatusCodeTracker>) -> Self {
        if code != 0 {
            tokio::spawn(async move {
                tracker.inc(code).await;
            });
        }
        self.with_http_status_code(code)
    }

}

impl std::fmt::Display for FlatRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let next_start = if self.next_start == "" {
            "/"
        } else {
            &self.next_start
        };
        write!(f, "errno: {}, msg: {}, next_start: {}", self.errno, self.errmsg, next_start)
    }
}
