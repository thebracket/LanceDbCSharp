//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

mod command;
mod helpers;
mod lifecycle;
mod connection;
mod errors;

use std::ffi::c_char;
use std::io::Cursor;
use crate::MAX_COMMANDS;
use std::sync::OnceLock;
use arrow_array::RecordBatchIterator;
use arrow_ipc::reader::FileReader;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handle::{ConnectionFactory, ConnectionHandle};
use crate::event_loop::command::LanceDbCommand;
use crate::batch_handler::BatchHandler;
use crate::blob_handler::BlobHandler;
use crate::event_loop::helpers::send_command;
use crate::table_handler::TableHandler;

pub use lifecycle::{setup, shutdown};
pub use connection::{connect, disconnect};
pub use errors::{get_error_message, free_error_message};
use crate::event_loop::errors::add_error;

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
                        let error_index = add_error(e.to_string());
                        send_reply(reply_sender, error_index).await;
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
            LanceDbCommand::DropDatabase { connection_handle, reply_sender } => {
                if let Some(cnn) = connection_factory.get_connection(ConnectionHandle(connection_handle)) {
                    match cnn.drop_db().await {
                        Ok(_) => {
                            send_reply(reply_sender, 0).await;
                        }
                        Err(e) => {
                            let error_index = add_error(e.to_string());
                            eprintln!("Error dropping database: {:?}", e);
                            send_reply(reply_sender, error_index).await;
                        }
                    }
                } else {
                    eprintln!("Connection handle {connection_handle} not found.");
                    send_reply(reply_sender, -1).await;
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
                            match table_handler.add_table(&name, cnn, data).await {
                                Ok(handle) => {
                                    result = handle;
                                }
                                Err(e) => {
                                    let error_index = add_error(e.to_string());
                                    eprintln!("Error creating table: {:?}", e);
                                    result = error_index;
                                }
                            }
                        }
                    } else {
                        eprintln!("Record batch handle {record_batch_handle} not found.");
                    }
                } else {
                    eprintln!("Connection handle {connection_handle} not found.");
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::OpenTable { name, connection_handle, reply_sender } => {
                let mut result = -1;
                if let Some(cnn) = connection_factory.get_connection(ConnectionHandle(connection_handle)) {
                    match table_handler.open_table(&name, cnn).await {
                        Ok(handle) => {
                            result = handle;
                        }
                        Err(e) => {
                            let error_index = add_error(e.to_string());
                            eprintln!("Error opening table: {:?}", e);
                            result = error_index;
                        }
                    }
                } else {
                    eprintln!("Connection handle {connection_handle} not found.");
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::DropTable { name, connection_handle, reply_sender } => {
                let mut result = -1;
                if let Some(cnn) = connection_factory.get_connection(ConnectionHandle(connection_handle)) {
                    match table_handler.drop_table(&name, cnn).await {
                        Ok(_) => {
                            result = 0;
                        }
                        Err(e) => {
                            let error_index = add_error(e.to_string());
                            eprintln!("Error dropping table: {:?}", e);
                            result = error_index;
                        }
                    }
                } else {
                    eprintln!("Connection handle {connection_handle} not found.");
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::QueryNearest { limit, vector, table_handle, reply_sender } => {
                let mut result = -1;
                if let Ok(table) = table_handler.get_table_from_cache(table_handle).await {
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
                    eprintln!("Table {table_handle} not found.");
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

/// Submit a record batch to batch storage. This does NOT submit it to the
/// database, it makes it available for use in other functions.
///
/// Be sure to call `free_record_batch` when you're done with the batch.
#[no_mangle]
pub extern "C" fn submit_record_batch(batch: *const u8, len: usize) -> i64 {
    // Convert "batch" to a slice of bytes
    let batch = unsafe { std::slice::from_raw_parts(batch, len) };

    match FileReader::try_new(Cursor::new(batch), None) {
        Ok(reader) => {
            let batches: Vec<_> = reader.collect::<Vec<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
            if send_command(LanceDbCommand::SendRecordBatch {
                batch: batches,
                reply_sender: reply_tx,
            }).is_err() {
                eprintln!("Error sending record batch command. Are we setup?");
                return -1;
            }
            reply_rx.blocking_recv().unwrap_or_else(|e| {
                eprintln!("Error receiving record batch response: {:?}", e);
                -1
            })
        }
        Err(e) => {
            let error_index = add_error(e.to_string());
            eprintln!("Error reading record batch: {:?}", e);
            error_index
        }
    }
}

/// Free a record batch from memory. This function should be called
/// when you're done with a record batch.
///
/// There's no harm in calling this on a batch that has already been
/// freed.
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

/// Create a table in the database. This function will create a table
/// with the given name, using the connection and record batch provided.
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

/// Open a table in the database. This function will open a table with
/// the given name, using the connection provided.
#[no_mangle]
pub extern "C" fn open_table(name: *const c_char, connection_handle: i64) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::OpenTable {
        name,
        connection_handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving open table response: {:?}", e);
        -1
    })
}

/// Drop a table from the database. This function will drop a table with
/// the given name, using the connection provided. WARNING: this invalidates
/// any cached table handles referencing the table.
#[no_mangle]
pub extern "C" fn drop_table(name: *const c_char, connection_handle: i64) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::DropTable {
        name,
        connection_handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving drop table response: {:?}", e);
        -1
    })
}

/// Drop a database from the connection. This function will drop the
/// database associated with the connection handle.
#[no_mangle]
pub extern "C" fn drop_database(connection_handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    if send_command(LanceDbCommand::DropDatabase {
        connection_handle,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving drop database response: {:?}", e);
        -1
    })
}

/// Query the database for the nearest records to a given vector.
#[no_mangle]
pub extern "C" fn query_nearest_to(limit: u64, vector: *const f32, vector_len: usize, table_handle: i64) -> i64 {
    // Convert "vector" to a vector of f32
    let vector = unsafe { std::slice::from_raw_parts(vector, vector_len) };
    let vector = vector.to_vec();

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<i64>();
    send_command(LanceDbCommand::QueryNearest {
        limit,
        vector,
        table_handle,
        reply_sender: reply_tx,
    }).unwrap_or_else(|e| {
        eprintln!("Error sending query nearest command: {:?}", e);
    });
    reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving create table response: {:?}", e);
        -1
    })
}

/// Free a blob from memory. This function should be called when you're
/// done with a blob.
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

/// Get the length of a blob. This is necessary because the C ABI
/// makes returning both a pointer and a length difficult, and "out"
/// parameters in FFI can be dangerous.
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

/// Get a pointer to the data in a blob. This function will return
/// a pointer to the data in the blob, or null if the blob does not
/// exist.
///
/// The data is guaranteed to be valid until `free_blob` is called.
///
/// Do NOT free the pointer returned by this function. It remains
/// owned by the Rust side.
///
/// Do NOT call `free_blob` while you are still using the pointer.
/// That will lead to undefined behavior.
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