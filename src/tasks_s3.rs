use std::time::Duration;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use tokio::time::{Instant, timeout_at};
use log::{info, debug, trace};
use crate::core;
use crate::core::{S3TaskContext, ObjectKey, ObjectPrefix, ObjectName, ObjectProps};
use crate::data_map;
use crate::error::*;

pub async fn flat_list_main_task(ctx: &S3TaskContext, start_prefix: &str,
        flat_concurrency: usize, hints: data_map::KeySpaceHints) -> () {
    flat_reactor_task(ctx, start_prefix, flat_concurrency, hints).await
}

// task to control concurrency of s3 flat list
async fn flat_reactor_task(ctx: &S3TaskContext, start_prefix: &str,
        flat_concurrency: usize, mut hints: data_map::KeySpaceHints) -> () {

    ctx.start();
    ctx.g_state.wait_to_start().await;

    info!("Flat List S3 Task - {} - started", ctx.s3_bucket_name);
    tokio::task::yield_now().await;

    let mut joins = Vec::new();
    let mut last_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    loop {

        // if we in low pressure
        if joins.len() < flat_concurrency {
            if let Some(pair) = hints.next() {
                let task_ctx = ctx.clone();
                let start_prefix = start_prefix.to_string();

                let h = tokio::task::spawn(async move {
                        let (start, end) = pair.to_task_input();
                        flat_list_run_to_complete(&task_ctx, &start_prefix, start, end).await;
                        pair
                    });
                joins.push(h);
                continue;
            }
        }

        if joins.len() == 0 {
            ctx.complete();
            info!("Flat List S3 Task - {} - completed", ctx.s3_bucket_name);
            tokio::time::sleep(tokio::time::Duration::from_secs(core::DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS)).await;
            break;
        }

        let mut waitings = Vec::new();
        while let Some(h) = joins.pop() {
            if h.is_finished() {
                match h.await {
                    Ok(pair) => {
                        hints.finish(pair.index());
                    },
                    Err(e) => {
                        panic!("task join handler error {:?}", e);
                    }
                }
            } else {
                waitings.push(h);
            }
        }

        joins = waitings;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        if now - last_ts > core::DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS {
            info!("Flat List S3 Task - {} - heart beat, {} pairs pending to complete", ctx.s3_bucket_name, joins.len());
            last_ts = now;
        }

        if ctx.is_quit() {
            while let Some(h) = joins.pop() {
                h.abort();
            }
            info!("Flat List S3 Task - {} - all handler aborted", ctx.s3_bucket_name);
            break;
        }
    }

    info!("Flat List S3 Task - {} - quit", ctx.s3_bucket_name);
    ()
}

async fn flat_list_run_to_complete(ctx: &S3TaskContext, prefix: &str, start: &str, until: Option<&str>) {

    let mut start_after = start.to_string();
    let prefix = prefix.to_string();
    while let Err(err) = flat_list(ctx, &prefix, &start_after, until).await {
        if err.continue_on_error() {
            start_after = err.next_start();
            continue;
        }
        if ctx.is_running() {
            info!("Flat List S3 Task - {} - unexpected quit - {}", ctx.s3_bucket_name, err);
            ctx.complete();
        }
        return;
    }
}

async fn flat_list(ctx: &S3TaskContext, prefix: &str, start_after: &str, until: Option<&str>) -> std::result::Result<(), FlatRuntimeError> {

    let mut stream = ctx.s3_client.list_objects_v2()
        .bucket(&ctx.s3_bucket_name)
        .prefix(prefix)
        .start_after(start_after)
        .into_paginator()
        .send();

    debug!("input pair start {}, end {:?}", start_after, until);
    let mut next_start = start_after.to_string();
    let mut is_ended = false;
    loop {

        let res = timeout_at(Instant::now() + Duration::from_secs(core::DEFAULT_S3_CLIENT_TIMEOUT), stream.next()).await;

        if res.is_err() {
            debug!("flat list timeout next_start: {}", next_start);
            ctx.g_state.inc_task_next_stream_timeout();
            return Err(FlatRuntimeError::new(ERROR_S3_NEXT_STREAM_TIMEOUT, "client timeout".to_string(), next_start));
        }

        let paginator = res.unwrap();
        if paginator.is_none() {
            break;
        }
        let response = paginator.unwrap();

        if let Err(sdk_err) = response {

            match &sdk_err {
                aws_sdk_s3::error::SdkError::ServiceError(err) => {
                    let errno;
                    match err.err() {
                        ListObjectsV2Error::NoSuchBucket(e) => {
                            trace!(" - {}", e);
                            errno = ERROR_S3_NO_BUCKET;
                        },
                        e @ _ => {
                            match e.meta().code() {
                                Some("AccessDenied") => {
                                    errno = ERROR_S3_ACCESS_DENIED;
                                },
                                Some("PermanentRedirect") => {
                                    errno = ERROR_S3_PERMANENT_REDIRECT;
                                },
                                Some(_) => {
                                    errno = ERROR_S3_UNKOWN;
                                },
                                None => {
                                    panic!("unkown error occurs from ListObjectsV2 {}", e);
                                },
                            }
                            trace!(" - {}", e);
                        },
                    }
                    let http_status_code = err.raw().status().as_u16();
                    return Err(
                        FlatRuntimeError::new(
                            errno,
                            err.err().meta().message().unwrap().to_string(),
                            next_start
                        )
                        .with_http_status_code_tracker(http_status_code, ctx.get_tracker())
                    );
                },
                aws_sdk_s3::error::SdkError::DispatchFailure(err) => {
                    if err.is_timeout() {
                        ctx.g_state.inc_s3_client_timeout();
                        return Err(
                           FlatRuntimeError::new(
                                ERROR_S3_CLIENT_CONNECTION_TIMEOUT,
                                err.as_connector_error().unwrap().to_string(),
                                next_start
                            )
                        );
                    }
                },
                _ => {
                    /* do nothing here, let below return handle everything else */
                }
            }
            ctx.g_state.inc_s3_client_generic_error();
            return Err(FlatRuntimeError::new(ERROR_S3_CLIENT_GENERIC, sdk_err.to_string(), next_start));
        }

        let objects = response.unwrap();
        let mut output: HashMap<ObjectPrefix, Vec<(ObjectName, ObjectProps)>> = HashMap::new();

        assert!(objects.contents().len() == objects.key_count.unwrap_or(0) as usize);
        let mut key_count = objects.key_count.unwrap_or(0);
        // collect data group by prefix
        for obj in objects.contents() {
            if let Some(obj_key) = obj.key() {

                let key: ObjectKey = obj_key.into();
                if let Some(end) = until {
                    // we need lexicographical compare here
                    // because "end" could be a non-exist key/prefix
                    if end <= key.as_str() {
                        debug!("pair end at key: {}", key.as_str());
                        is_ended = true;
                        break;
                    }
                }

                // remember last key to next start for failsafe
                if key_count == 1 {
                    next_start = obj_key.to_string();
                }

                let (prefix, name) = key.decode();
                let mut props: ObjectProps = obj.into();
                props.set_dir(ctx.dir);

                if let Some(v) = output.get_mut(&prefix) {
                    v.push((name, props));
                } else {
                    let mut v = Vec::new();
                    v.push((name, props));
                    output.insert(prefix, v);
                }
            }
            key_count -= 1;
        }

        if let Err(e) = ctx.data_map_channel.send(output) {
            if ctx.is_quit() != true {
                panic!("error on send data to data map channel"); 
            }
            panic!("failed to send output data to data map channel, err: {}", e);
        }

        if is_ended {
            break;
        }
    }
    debug!("finished pair start {}, end {:?}", start_after, until);

    Ok(())
}
