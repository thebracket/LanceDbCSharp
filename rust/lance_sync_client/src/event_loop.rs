//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

use std::ffi::c_char;
use crate::MAX_COMMANDS;
use std::sync::OnceLock;
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handle::{ConnectionFactory, ConnectionHandle};

/// This enum represents the possible errors that can occur during
/// the setup of the LanceDB event-loop.
#[repr(i32)]
enum LanceSetupErrors {
    Ok = 0,
    ThreadSpawnError = -1,
}

/// Commands that can be sent to the LanceDB event-loop.
pub(crate) enum LanceDbCommand {
    /// Request to create a new connection to the database.
    ConnectionRequest{
        uri: String,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },
    /// Request to disconnect a connection from the database.
    Disconnect{
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },
    /// Gracefully shut down the event-loop.
    Quit,
}

/// This static variable holds the sender for the LanceDB command.
pub(crate) static COMMAND_SENDER: OnceLock<Sender<LanceDbCommand>> = OnceLock::new();

async fn event_loop() {
    let (tx, mut rx) = channel::<LanceDbCommand>(MAX_COMMANDS);
    if let Err(e) = COMMAND_SENDER.set(tx) {
        eprintln!("Error setting up command sender: {:?}", e);
        return;
    }

    // Create a connection factory to handle mapping handles to connections
    let mut connection_factory = ConnectionFactory::new();

    while let Some(command) = rx.recv().await {
        match command {
            LanceDbCommand::ConnectionRequest{uri, reply_sender} => {
                let result = connection_factory.create_connection(&uri).await;
                match result {
                    Ok(handle) => {
                        if let Err(e) = reply_sender.send(handle.0) {
                            eprintln!("Error sending connection handle: {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error creating connection: {:?}", e);
                        if let Err(e) = reply_sender.send(-1) {
                            eprintln!("Error sending connection handle: {:?}", e);
                        }
                    }
                }
            }
            LanceDbCommand::Disconnect { handle, reply_sender } => {
                let result = connection_factory.disconnect(ConnectionHandle(handle));
                match result {
                    Ok(_) => {
                        if let Err(e) = reply_sender.send(0) {
                            eprintln!("Error sending disconnect reply: {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error disconnecting: {:?}", e);
                        if let Err(e) = reply_sender.send(-1) {
                            eprintln!("Error sending disconnect reply: {:?}", e);
                        }
                    }
                }
            }
            LanceDbCommand::Quit => {
                println!("Received quit command. Shutting down.");
                break;
            }
        }
    }
    println!("Event loop shutting down.");
}

/// Spawns a new thread and starts an event-loop ready
/// to work with LanceDB. This function **must** be called before other
/// functions in this library are called.
///
/// Return values: 0 for success, -1 if an error occurred.
#[no_mangle]
pub extern "C" fn setup() -> i32 {
    let result = std::thread::Builder::new()
        .name("lance_sync_client".to_string())
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap()
                .block_on(event_loop());
        });

    match result {
        Ok(_) => LanceSetupErrors::Ok as i32,
        Err(e) => {
            eprintln!("Error spawning thread: {:?}", e);
            LanceSetupErrors::ThreadSpawnError as i32
        }
    }
}

/// Shuts down the event-loop. This function should be called
/// before the program exits (or the library is unloaded).
/// In practice, regular tear-down will stop the event-loop
/// anyway - but this avoids any leakage.
#[no_mangle]
pub extern "C" fn shutdown() {
    if let Some(tx) = COMMAND_SENDER.get() {
        if let Err(e) = tx.blocking_send(LanceDbCommand::Quit) {
            eprintln!("Error sending quit command: {:?}", e);
        }
    }
}

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
    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(LanceDbCommand::ConnectionRequest {
            uri,
            reply_sender: reply_tx,
        }).unwrap();

        let reply = reply_rx.blocking_recv().unwrap();
        reply
    } else {
        eprintln!("Command sender not set up.");
        return -1;
    }
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
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(LanceDbCommand::Disconnect {
            handle,
            reply_sender: reply_tx,
        }).unwrap();
        reply_rx.blocking_recv().unwrap()
    } else {
        eprintln!("Command sender not set up.");
        -1
    }
}