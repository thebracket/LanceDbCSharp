//! Provides a global error string table. As much as I dislike globals,
//! this allows errors to be tracked even if they are returned from
//! different threads or outside the event loop.

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Mutex;
use once_cell::sync::Lazy;

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