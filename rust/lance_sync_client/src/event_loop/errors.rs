//! Provides a global error string table. As much as I dislike globals,
//! this allows errors to be tracked even if they are returned from
//! different threads or outside the event loop.

use std::ffi::c_char;
use crate::event_loop::command::CompletionSender;

/// Type signature for error reporting callbacks.
pub(crate) type ErrorReportFn = extern "C" fn(i64, *const c_char);

/// Utilize the error reporting callback to report a result.
pub(crate) fn report_result(result: Result<i64, String>, target: ErrorReportFn, completion_sender: Option<CompletionSender>) {
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
        completion_sender.send(()).unwrap();
    }
}
