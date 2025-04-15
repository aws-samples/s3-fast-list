use std::time::Duration;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use tokio::time::{Instant, timeout_at};
use log::{info, debug, error};
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

    // Build the request with more detailed debug information
    let request = ctx.s3_client.list_objects_v2()
        .bucket(&ctx.s3_bucket_name)
        .prefix(prefix)
        .start_after(start_after);

    // Debug log the request details
    debug!("Sending S3 request: bucket={}, prefix={}, start_after={}",
           &ctx.s3_bucket_name, prefix, start_after);

    // Create the paginator
    let mut stream = request.into_paginator().send();

    debug!("input pair start {}, end {:?}", start_after, until);
    let mut next_start = start_after.to_string();
    let mut is_ended = false;
    loop {

        let timeout_duration = Duration::from_secs(core::DEFAULT_S3_CLIENT_TIMEOUT);
        debug!("Waiting for S3 response with timeout of {} seconds", timeout_duration.as_secs());
        let res = timeout_at(Instant::now() + timeout_duration, stream.next()).await;

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
            // Log detailed error information
            error!("S3 API error: {:?}", sdk_err);

            match &sdk_err {
                aws_sdk_s3::error::SdkError::ServiceError(err) => {
                    let errno;
                    match err.err() {
                        ListObjectsV2Error::NoSuchBucket(e) => {
                            error!("NoSuchBucket error: {}", e);
                            errno = ERROR_S3_NO_BUCKET;
                        },
                        e @ _ => {
                            let code = e.meta().code();
                            error!("Service error code: {:?}, message: {:?}",
                                   code, e.meta().message());

                            match code {
                                Some("AccessDenied") => {
                                    errno = ERROR_S3_ACCESS_DENIED;
                                },
                                Some("PermanentRedirect") => {
                                    errno = ERROR_S3_PERMANENT_REDIRECT;
                                },
                                Some(other_code) => {
                                    error!("Unknown service error code: {}", other_code);
                                    errno = ERROR_S3_UNKOWN;
                                },
                                None => {
                                    error!("Service error without code: {}", e);
                                    panic!("unknown error occurs from ListObjectsV2 {}", e);
                                },
                            }
                            error!("Full error details: {}", e);
                        },
                    }
                    let http_status_code = err.raw().status().as_u16();
                    error!("HTTP status code: {}", http_status_code);
                    return Err(
                        FlatRuntimeError::new(
                            errno,
                            err.err().meta().message().unwrap_or("No error message").to_string(),
                            next_start
                        )
                        .with_http_status_code_tracker(http_status_code, ctx.get_tracker())
                    );
                },
                aws_sdk_s3::error::SdkError::DispatchFailure(err) => {
                    error!("Dispatch failure: {:?}", err);

                    // Check for region-related errors
                    if let Some(conn_err) = err.as_connector_error() {
                        let err_str = conn_err.to_string();
                        if err_str.contains("region must be set") {
                            error!("Region error: A region must be set when using S3");
                            error!("Fix: Set region using --region parameter, AWS_REGION environment variable, or in AWS profile");
                            return Err(
                                FlatRuntimeError::new(
                                    ERROR_S3_MISSING_REGION,
                                    "Region must be set when using S3. Fix: Use --region parameter, AWS_REGION env var, or set in AWS profile.".to_string(),
                                    next_start
                                )
                            );
                        }
                    }

                    if err.is_timeout() {
                        if let Some(conn_err) = err.as_connector_error() {
                            error!("Connection timeout error: {}", conn_err);
                            ctx.g_state.inc_s3_client_timeout();
                            return Err(
                               FlatRuntimeError::new(
                                    ERROR_S3_CLIENT_CONNECTION_TIMEOUT,
                                    conn_err.to_string(),
                                    next_start
                                )
                            );
                        } else {
                            error!("Connection timeout but no connector error");
                            ctx.g_state.inc_s3_client_timeout();
                            return Err(
                               FlatRuntimeError::new(
                                    ERROR_S3_CLIENT_CONNECTION_TIMEOUT,
                                    "Unknown timeout error".to_string(),
                                    next_start
                                )
                            );
                        }
                    }
                },
                aws_sdk_s3::error::SdkError::ResponseError(err) => {
                    error!("Response error: {:?}", err);
                },
                aws_sdk_s3::error::SdkError::TimeoutError(err) => {
                    error!("Timeout error: {:?}", err);
                },
                aws_sdk_s3::error::SdkError::ConstructionFailure(err) => {
                    error!("Construction failure: {:?}", err);
                },
                _ => {
                    error!("Other SDK error type: {:?}", sdk_err);
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
