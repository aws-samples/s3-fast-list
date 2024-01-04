use log::info;
use crate::core::{MonContext, DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS};

pub async fn mon_task(ctx: MonContext) -> () {

    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Mon Task - started");

    loop {
        if ctx.is_quit() {
            ctx.complete();
            info!("Mon Task - quit");
            return ();
        }

        let tracker_stats = format!("{}", ctx.get_tracker());
        if tracker_stats.len() > 0 {
            info!("Mon Task - http status: {}", tracker_stats);
        }

        let task_next_stream_timeout = ctx.g_state.read_task_next_stream_timeout();
        let s3_client_timeout = ctx.g_state.read_s3_client_timeout();
        let s3_client_generic_error = ctx.g_state.read_s3_client_generic_error();
        if task_next_stream_timeout > 0 || s3_client_timeout > 0 || s3_client_generic_error > 0 {
            info!("Mon Task - next stream timeout: {}, s3 client timeout: {}, s3 client generic error: {}",
                task_next_stream_timeout, s3_client_timeout, s3_client_generic_error);
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS)).await;
    }
}
