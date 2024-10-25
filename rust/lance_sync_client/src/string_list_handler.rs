//! Passing lists of strings is particularly error-prone with FFI, so this handler
//! is provided to make it easier to pass lists of strings to the FFI consumer.

use std::collections::HashMap;
use std::ffi::{c_char, CString};
use std::sync::atomic::AtomicI64;
use std::sync::Mutex;
use anyhow::Result;
use once_cell::sync::Lazy;

struct CStringArray {
    strings: Vec<CString>,
    array: Box<[*const c_char]>,
    len: usize,
}

// TODO: This is a temporary hack. FIXME.
unsafe impl Send for CStringArray {}

impl CStringArray {
    fn from(strings: Vec<String>) -> Self {
        let c_strings: Vec<CString> = strings.iter().map(|s| std::ffi::CString::new(s.as_str()).unwrap()).collect();
        let c_ptrs: Vec<*const c_char> = c_strings.iter().map(|s| s.as_ptr()).collect();
        let array = c_ptrs.into_boxed_slice();
        Self {
            strings: c_strings,
            array,
            len: strings.len()
        }
    }
}

struct StringListHandler {
    next_id: AtomicI64,
    string_lists: Mutex<HashMap<i64, CStringArray>>
}

impl StringListHandler {
    fn new() -> Self {
        Self {
            next_id: AtomicI64::new(0),
            string_lists: Mutex::new(HashMap::new())
        }
    }

    fn add_string_list(&self, strings: Vec<String>) -> Result<i64> {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let c_string_array = CStringArray::from(strings);
        let mut lock = self.string_lists.lock().unwrap();
        lock.insert(id, c_string_array);
        Ok(id)
    }

    fn free_string_list(&self, id: i64) {
        let mut lock = self.string_lists.lock().unwrap();
        lock.remove(&id);
    }
}

// This is a static to keep the interface sane.
static STRING_LISTS: Lazy<StringListHandler> = Lazy::new(|| StringListHandler::new());

#[no_mangle]
pub extern "C" fn get_string_list(id: i64, out_len: *mut usize) -> *const *const c_char {
    let lock = STRING_LISTS.string_lists.lock().unwrap();
    let c_string_array = lock.get(&id).unwrap();
    unsafe {
        *out_len = c_string_array.len;
        c_string_array.array.as_ptr()
    }
}

#[no_mangle]
pub extern "C" fn free_string_list(id: i64) {
    STRING_LISTS.free_string_list(id);
}

pub fn add_string_list(strings: Vec<String>) -> Result<i64> {
    STRING_LISTS.add_string_list(strings)
}