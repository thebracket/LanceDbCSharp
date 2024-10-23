//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

mod command;

use std::ffi::c_char;
use std::io::Cursor;
use crate::MAX_COMMANDS;
use std::sync::OnceLock;
use anyhow::Context;
use arrow_array::RecordBatchIterator;
use arrow_ipc::reader::FileReader;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handle::{ConnectionFactory, ConnectionHandle};
use crate::event_loop::command::LanceDbCommand;
use crate::batch_handler::BatchHandler;
use crate::blob_handler::BlobHandler;
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

async fn send_reply<T>(tx: tokio::sync::oneshot::Sender<T>, response: T)
where T: std::fmt::Debug
{
    if let Err(e) = tx.send(response) {
        eprintln!("Error sending reply: {:?}", e);
    }
}

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

    // Blob handler. Sometimes we're returning byte arrays, and we need to ensure
    // they remain valid until they're no longer needed.
    let mut blob_handler = BlobHandler::new();

    while let Some(command) = rx.recv().await {
        match command {
            LanceDbCommand::ConnectionRequest{uri, reply_sender} => {
                let result = connection_factory.create_connection(&uri).await;
                match result {
                    Ok(handle) => {
                        send_reply(reply_sender, handle.0).await;
                    }
                    Err(e) => {
                        eprintln!("Error creating connection: {:?}", e);
                        send_reply(reply_sender, -1).await;
                    }
                }
            }
            LanceDbCommand::Disconnect { handle, reply_sender } => {
                let result = connection_factory.disconnect(ConnectionHandle(handle));
                match result {
                    Ok(_) => {
                        send_reply(reply_sender, 0).await;
                    }
                    Err(e) => {
                        eprintln!("Error disconnecting: {:?}", e);
                        send_reply(reply_sender, -1).await;
                    }
                }
            }
            LanceDbCommand::SendRecordBatch { batch, reply_sender } => {
                let handle = batch_handler.add_batch(batch);
                send_reply(reply_sender, handle).await;
            }
            LanceDbCommand::FreeRecordBatch { handle, reply_sender } => {
                batch_handler.free_if_exists(handle);
                send_reply(reply_sender, 0).await;
            }
            LanceDbCommand::CreateTable { name, connection_handle, record_batch_handle, reply_sender,  } => {
                let mut result = -1;
                if let Some(cnn) = connection_factory.get_connection(ConnectionHandle(connection_handle)) {
                    if let Some(data) = batch_handler.take_batch(record_batch_handle) {
                        // TODO: Accessing the 1st record is temporary; we need to be storing the schema with the batch
                        if let Ok(record) = data[0].as_ref() {
                            let schema = record.schema().clone();
                            let data = RecordBatchIterator::new(data, schema);
                            if let Err(e) = table_handler.add_table(&name, cnn, data).await {
                                eprintln!("Error creating table: {:?}", e);
                            }
                            result = 0;
                        }
                    } else {
                        eprintln!("Record batch handle {record_batch_handle} not found.");
                    }
                } else {
                    eprintln!("Connection handle {connection_handle} not found.");
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::QueryNearest { limit, vector, connection, table, reply_sender } => {
                let mut result = -1;
                if let Some(cnn) = connection_factory.get_connection(connection) {
                    if let Ok(table) = table_handler.get_table(cnn, &table).await {
                        if let Ok(query_builder) = table
                            .query()
                            .limit(limit as usize)
                            .nearest_to(vector) {
                            if let Ok(query) = query_builder.execute().await {
                                let records = query.try_collect::<Vec<_>>()
                                    .await;
                                if let Ok(records) = records {
                                    let batch = records
                                        .into_iter()
                                        .map(|r| Ok(r))
                                        .collect::<Vec<_>>();
                                    // TODO: This is really inefficient
                                    let batch_handle = batch_handler.add_batch(batch);
                                    let blob = batch_handler.batch_as_bytes(batch_handle);
                                    let blob_handle = blob_handler.add_blob(blob);
                                    result = blob_handle;
                                }
                                // Do something with it
                                //println!("{:?}", query.try_collect::<Vec<_>>().await);
                            } else {
                                eprintln!("Error executing query.");
                            }
                        } else {
                            eprintln!("Error creating query.");
                        }
                    } else {
                        eprintln!("Table {table} not found.");
                    }
                } else {
                    eprintln!("Connection handle {} not found.", connection.0);
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::FreeBlob { handle, reply_sender } => {
                blob_handler.free_if_exists(handle);
                send_reply(reply_sender, 0).await;
            }
            LanceDbCommand::BlobLen { handle, reply_sender } => {
                let len = blob_handler.blob_len(handle);
                send_reply(reply_sender, len).await;
            }
            LanceDbCommand::GetBlobPointer { handle, reply_sender } => {
                let ptr = blob_handler.get_blob_arc(handle);
                send_reply(reply_sender, ptr).await;
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
            match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build() {
                Ok(runtime) => {
                    runtime.block_on(event_loop());
                }
                Err(e) => {
                    eprintln!("Error creating runtime: {:?}", e);
                }
            }
        });

    match result {
        Ok(_) => LanceSetupErrors::Ok as i32,
        Err(e) => {
            eprintln!("Error spawning thread: {:?}", e);
            LanceSetupErrors::ThreadSpawnError as i32
        }
    }
}

fn send_command(command: LanceDbCommand) -> anyhow::Result<()> {
    let tx = COMMAND_SENDER.get()
        .context("Command sender not set up - command channel is closed.")?;
    tx.blocking_send(command)
        .inspect_err(|e| eprintln!("Error sending command: {:?}", e))?;
    Ok(())
}

/// Shuts down the event-loop. This function should be called
/// before the program exits (or the library is unloaded).
/// In practice, regular tear-down will stop the event-loop
/// anyway - but this avoids any leakage.
#[no_mangle]
pub extern "C" fn shutdown() -> i32 {
    match send_command(LanceDbCommand::Quit) {
        Ok(_) => 0,
        Err(_) => -1,
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
    eprintln!("Connecting to: {uri}");

    if send_command(LanceDbCommand::ConnectionRequest {
        uri,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    };

    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving connection handle: {:?}", e);
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
        if send_command(LanceDbCommand::SendRecordBatch {
            batch: batches,
            reply_sender: reply_tx,
        }).is_err() {
            return -1;
        }
        return reply_rx.blocking_recv().unwrap_or_else(|e| {
            eprintln!("Error receiving record batch response: {:?}", e);
            -1
        });
    }
    0
}

#[no_mangle]
pub extern "C" fn free_record_batch(handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::FreeRecordBatch {
        handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving free record batch response: {:?}", e);
        -1
    })
}

#[no_mangle]
pub extern "C" fn create_table(name: *const c_char, connection_handle: i64, record_batch_handle: i64) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::CreateTable {
        name,
        connection_handle,
        record_batch_handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving create table response: {:?}", e);
        -1
    })
}

#[no_mangle]
pub extern "C" fn query_nearest_to(connection_handle: i64, limit: u64, vector: *const f32, vector_len: usize, table_name: *const c_char) -> i64 {
    // Convert "vector" to a vector of f32
    let vector = unsafe { std::slice::from_raw_parts(vector, vector_len) };
    let vector = vector.to_vec();

    // Convert the table name to a string
    let table_name = unsafe { std::ffi::CStr::from_ptr(table_name).to_string_lossy().to_string() };

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    send_command(LanceDbCommand::QueryNearest {
        limit,
        vector,
        connection: ConnectionHandle(connection_handle),
        table: table_name,
        reply_sender: reply_tx,
    }).unwrap_or_else(|e| {
        eprintln!("Error sending query nearest command: {:?}", e);
    });
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving create table response: {:?}", e);
        -1
    })
}

#[no_mangle]
pub extern "C" fn free_blob(handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::FreeBlob {
        handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving free blob response: {:?}", e);
        -1
    })
}

#[no_mangle]
pub extern "C" fn blob_len(handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Option<isize>>();
    if send_command(LanceDbCommand::BlobLen {
        handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let reply = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving blob length response: {:?}", e);
        None
    });
    reply.unwrap_or(-1) as i64
}

#[no_mangle]
pub extern "C" fn get_blob_data(handle: i64) -> *const u8 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Option<std::sync::Arc<Vec<u8>>>>();
    if send_command(LanceDbCommand::GetBlobPointer {
        handle,
        reply_sender: reply_tx,
    }).is_err() {
        return std::ptr::null();
    }

    let reply = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving blob data response: {:?}", e);
        None
    });
    if let Some(arc) = reply {
        let ptr = arc.as_ptr();
        ptr
    } else {
        std::ptr::null()
    }
}