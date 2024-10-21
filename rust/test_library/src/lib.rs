//! A truly minimal test library to verify that FFI is working
//! between Rust and C#.
//!
//! This library will build in the "target/debug" directory. The
//! simple FFI example is using relative paths for now.

use std::ffi::c_char;

/// Adds two integers together.
#[no_mangle] // Disable name mangling
pub extern "C" fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Prints a string to the console. This is present to make sure
/// that the slightly more complex handling of C-Strings between
/// the two environments is happy.
#[no_mangle]
pub extern "C" fn print(text: *const c_char) {
    use std::ffi::CStr;
    let c_str = unsafe { CStr::from_ptr(text) };
    let str_slice = c_str.to_str().unwrap();
    println!("{}", str_slice);
}