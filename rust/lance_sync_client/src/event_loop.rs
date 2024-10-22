//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

mod command;

use std::ffi::c_char;
use std::io::Cursor;
use crate::MAX_COMMANDS;
use std::sync::OnceLock;
use arrow_array::RecordBatchIterator;
use arrow_ipc::reader::FileReader;
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handle::{ConnectionFactory, ConnectionHandle};
use crate::event_loop::command::LanceDbCommand;
use crate::batch_handler::BatchHandler;
use crate::table_handler::TableHandler;

/// This enum represents the possible errors that can occur during
/// the setup of the LanceDB event-loop.
#[repr(i32)]
enum LanceSetupErrors {
    Ok = 0,
    ThreadSpawnError = -1,
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

    // Create a manager that handles record batches
    let mut batch_handler = BatchHandler::new();

    // Table handler
    let mut table_handler = TableHandler::new();

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
            LanceDbCommand::SendRecordBatch { batch, reply_sender } => {
                let handle = batch_handler.add_batch(batch);
                reply_sender.send(handle).unwrap();
            }
            LanceDbCommand::FreeRecordBatch { handle, reply_sender } => {
                batch_handler.free_if_exists(handle);
                reply_sender.send(0).unwrap();
            }
            LanceDbCommand::CreateTable { name, connection_handle, record_batch_handle, reply_sender,  } => {
                let cnn = connection_factory.get_connection(ConnectionHandle(connection_handle)).unwrap();
                let data = batch_handler.take_batch(record_batch_handle).unwrap();
                let schema = data[0].as_ref().unwrap().schema().clone();
                let data = RecordBatchIterator::new(data, schema);
                table_handler.add_table(&name, cnn, data).await;
                reply_sender.send(0).unwrap();
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

#[no_mangle]
pub extern "C" fn submit_record_batch(batch: *const u8, len: usize) -> i64 {
    // Convert "batch" to a slice of bytes
    let batch = unsafe { std::slice::from_raw_parts(batch, len) };
    if let Ok(reader) = FileReader::try_new(Cursor::new(batch), None) {
        println!("{:?}", reader.schema());
        let batches: Vec<_> = reader.collect::<Vec<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        println!("Received {} record batches.", batches.len());

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
        if let Some(tx) = COMMAND_SENDER.get() {
            tx.blocking_send(LanceDbCommand::SendRecordBatch {
                batch: batches,
                reply_sender: reply_tx,
            }).unwrap();
            return reply_rx.blocking_recv().unwrap();
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn free_record_batch(handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(LanceDbCommand::FreeRecordBatch {
            handle,
            reply_sender: reply_tx,
        }).unwrap();
        reply_rx.blocking_recv().unwrap()
    } else {
        eprintln!("Command sender not set up.");
        -1
    }
}

#[no_mangle]
pub extern "C" fn create_table(name: *const c_char, connection_handle: i64, record_batch_handle: i64) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(LanceDbCommand::CreateTable {
            name,
            connection_handle,
            record_batch_handle,
            reply_sender: reply_tx,
        }).unwrap();
        reply_rx.blocking_recv().unwrap()
    } else {
        eprintln!("Command sender not set up.");
        -1
    }
}