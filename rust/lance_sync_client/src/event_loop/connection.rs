use std::ffi::c_char;
use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::helpers::send_command;

/// Connect to a LanceDB database. This function will return a handle
/// to the connection, which can be used in other functions.
///
/// Parameters:
/// - `uri`: The URI to connect to.
///
/// Return values:
/// - A handle to the connection, or -1 if an error occurred.
#[no_mangle]
pub extern "C" fn connect(uri: *const c_char) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let uri = unsafe { std::ffi::CStr::from_ptr(uri).to_string_lossy().to_string() };

    if send_command(LanceDbCommand::ConnectionRequest {
        uri,
        reply_sender: reply_tx,
    }).is_err() {
        println!("Error sending connection request.");
        return -1;
    };

    reply_rx.blocking_recv().unwrap_or_else(|e| {
        println!("Error receiving connection handle: {:?}", e);
        -1
    })
}

/// Disconnect from a LanceDB database. This function will close the
/// connection associated with the handle.
///
/// Parameters:
/// - `handle`: The handle to the connection to disconnect.
///
/// Return values:
/// - 0 if the disconnection was successful, -1 if an error occurred.
#[no_mangle]
pub extern "C" fn disconnect(handle: i64) -> i64 {
    println!("(RUST) Disconnect called for handle {}", handle);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::Disconnect {
        handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }

    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving disconnection response: {:?}", e);
        -1
    })
}