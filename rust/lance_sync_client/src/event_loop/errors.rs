//! Provides a global error string table. As much as I dislike globals,
//! this allows errors to be tracked even if they are returned from
//! different threads or outside the event loop.

use std::collections::HashMap;
use std::ffi::c_char;
use std::sync::atomic::AtomicI64;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use crate::event_loop::command::CompletionSender;

/// Type signature for error reporting callbacks.
pub(crate) type ErrorReportFn = fn(i64, *const c_char);

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

static ERROR_INDEX: AtomicI64 = AtomicI64::new(-2);
static ERROR_TABLE: Lazy<Mutex<HashMap<i64, String>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Adds an error to the global error table, returning the index.
pub(crate) fn add_error(error: String) -> i64 {
    let index = ERROR_INDEX.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    ERROR_TABLE.lock().unwrap().insert(index, error);
    index
}

/// Retrieves an error from the global error table.
pub(crate) fn get_error(index: i64) -> Option<String> {
    ERROR_TABLE.lock().unwrap().get(&index).cloned()
}

/// Removes an error from the global error table.
pub(crate) fn remove_error(index: i64) {
    ERROR_TABLE.lock().unwrap().remove(&index);
}

/// Clears the global error table.
pub(crate) fn clear_errors() {
    ERROR_TABLE.lock().unwrap().clear();
}

/// External FFI function to retrieve an error message by index.
#[no_mangle]
pub extern "C" fn get_error_message(index: i64) -> *const std::os::raw::c_char {
    let error = get_error(index).unwrap_or_else(|| "Error not found".to_string());
    std::ffi::CString::new(error).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn free_error_message(index: i64) {
    remove_error(index);
}