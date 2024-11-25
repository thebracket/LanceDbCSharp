//! Provides a global error string table. As much as I dislike globals,
//! this allows errors to be tracked even if they are returned from
//! different threads or outside the event loop.

use crate::event_loop::command::CompletionSender;
use std::ffi::c_char;
use tokio::task::spawn_blocking;

/// Type signature for error reporting callbacks.
pub(crate) type ErrorReportFn = extern "C" fn(i64, *const c_char);

/// Utilize the error reporting callback to report a result.
pub(crate) async fn report_result(
    result: Result<i64, String>,
    target: ErrorReportFn,
    completion_sender: Option<CompletionSender>,
) {
    if let Some(completion_sender) = completion_sender {
        match result {
            Ok(code) => {
                completion_sender.send((code, String::new())).unwrap();
            }
            Err(error) => {
                completion_sender.send((-1, error)).unwrap();
            }
        }
    } else {
        match result {
            Ok(code) => {
                spawn_blocking(move || {
                    target(code, std::ptr::null());
                });
            }
            Err(error) => {
                let error_string = std::ffi::CString::new(error).unwrap();
                spawn_blocking(move || {
                    target(-1, error_string.as_ptr());
                });
            }
        }
    }
}

pub(crate) fn report_result_sync(
    result: Result<i64, String>,
    target: ErrorReportFn,
    completion_sender: Option<CompletionSender>,
) {
    match result {
        Ok(code) => {
            target(code, std::ptr::null());
        }
        Err(error) => {
            let error_string = std::ffi::CString::new(error).unwrap();
            target(-1, error_string.as_ptr());
        }
    }
    if let Some(completion_sender) = completion_sender {
        completion_sender.send((0, String::new())).unwrap();
    }
}