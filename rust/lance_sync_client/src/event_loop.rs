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
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handler::{ConnectionActor, ConnectionCommand, ConnectionHandle};
use crate::event_loop::command::{get_completion_pair, LanceDbCommand};
use crate::batch_handler::{BatchHandler, RecordBatchHandle};
use crate::blob_handler::{BlobHandle, BlobHandler};
use crate::event_loop::helpers::send_command;
use crate::table_handler::{TableActor, TableCommand, TableHandle, TableHandler};

pub(crate) use lifecycle::setup;
pub use connection::{connect, disconnect};
pub use errors::{get_error_message, free_error_message};
pub(crate) use errors::{ErrorReportFn, report_result, add_error};
pub(crate) use command::CompletionSender;
pub(crate) use connection::get_connection;
use crate::event_loop::connection::{do_connection_request, do_create_table_with_schema, do_disconnect, do_drop_database, do_list_tables, do_open_table, get_table};
use crate::serialization::{bytes_to_record_batch, schema_to_bytes};

/// This static variable holds the sender for the LanceDB command.
pub(crate) static COMMAND_SENDER: OnceLock<Sender<LanceDbCommand>> = OnceLock::new();

async fn send_reply<T>(tx: tokio::sync::oneshot::Sender<T>, response: T)
where T: std::fmt::Debug
{
    if let Err(e) = tx.send(response) {
        eprintln!("Error sending reply: {:?}", e);
    }
}

async fn event_loop(ready_tx: tokio::sync::oneshot::Sender<()>) {
    let (tx, mut rx) = channel::<LanceDbCommand>(MAX_COMMANDS);
    if let Err(e) = COMMAND_SENDER.set(tx) {
        eprintln!("Error setting up command sender: {:?}", e);
        return;
    }

    // Create a connection factory to handle mapping handles to connections
    let connections = ConnectionActor::start().await;

    // Table handler
    let tables = TableActor::start().await;

    ready_tx.send(()).unwrap();
    let mut quit_sender = None;
    while let Some(command) = rx.recv().await {
        match command {
            LanceDbCommand::ConnectionRequest{uri, reply_sender, completion_sender} => {
                tokio::spawn(do_connection_request(connections.clone(), uri, reply_sender, completion_sender));
            }
            LanceDbCommand::Disconnect { handle, reply_sender, completion_sender, } => {
                tokio::spawn(do_disconnect(connections.clone(), handle, reply_sender, completion_sender));
            }
            LanceDbCommand::DropDatabase { connection_handle, reply_sender, completion_sender } => {
                tokio::spawn(do_drop_database(connections.clone(), connection_handle, reply_sender, completion_sender));
            }
            LanceDbCommand::CreateTableWithSchema { name, connection_handle, schema, reply_sender, completion_sender } => {
                tokio::spawn(do_create_table_with_schema(connections.clone(), tables.clone(), connection_handle, name, schema, reply_sender, completion_sender));
            }
            LanceDbCommand::OpenTable { name, connection_handle, reply_sender, completion_sender } => {
                tokio::spawn(do_open_table(tables.clone(), connections.clone(), name, connection_handle, reply_sender, completion_sender));
            }
            LanceDbCommand::ListTableNames { connection_handle, reply_sender, completion_sender, string_callback } => {
                do_list_tables(connections.clone(), connection_handle, reply_sender, completion_sender, string_callback).await;
            }
            LanceDbCommand::DropTable { name, connection_handle, reply_sender, completion_sender } => {
                let (tx, rx) = get_completion_pair();
                if send_command(LanceDbCommand::DropTable {
                    connection_handle,
                    name,
                    reply_sender: reply_sender.clone(),
                    completion_sender: tx,
                }).is_err() {
                    report_result(Err("Error sending drop table request.".to_string()), reply_sender, Some(completion_sender));
                    continue;
                }
                rx.await.unwrap();
            }
            LanceDbCommand::CloseTable { connection_handle, table_handle, reply_sender } => {
                table_handler.release_table_handle(table_handle).await;
                send_reply(reply_sender, Ok(())).await;
            }
            LanceDbCommand::CountRows { connection_handle, table_handle, reply_sender } => {
                let mut result = Err(-1);
                if let Some(cnn) = get_connection(connections.clone(), connection_handle).await {
                    if let Ok(table) = get_table(tables.clone(), table_handle).await {
                        match table.count_rows(None).await {
                            Ok(count) => {
                                result = Ok(count as u64);
                            }
                            Err(e) => {
                                let error_index = add_error(e.to_string());
                                eprintln!("Error counting rows: {:?}", e);
                                result = Err(error_index);
                            }
                        }
                    }
                } else {
                    eprintln!("Connection handle {} not found.", connection_handle.0);
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::AddRows { connection_handle, table_handle, record_batch, reply_sender } => {
                let mut result = Err(-1);
                if let Some(cnn) = get_connection(connections.clone(), connection_handle).await {
                    if let Ok(table) = get_table(tables.clone(), table_handle).await {
                        if let Ok(schema) = table.schema().await {
                            let data = RecordBatchIterator::new(record_batch, schema);
                            if let Err(e) = table.add(data).execute().await {
                                eprintln!("Error adding rows: {:?}", e);
                                let error_index = add_error(e.to_string());
                                result = Err(error_index);
                            } else {
                                result = Ok(());
                            }
                        }
                    }
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::CreateScalarIndex { connection_handle, table_handle, column_name, index_type, replace, reply_sender } => {
                let mut result = Err(-1);
                if let Ok(table) = get_table(tables.clone(), table_handle).await {
                    // TODO: Need to support different index types
                    match table.create_index(&[column_name], lancedb::index::Index::Auto).execute().await {
                        Ok(_) => {
                            result = Ok(());
                        }
                        Err(e) => {
                            let error_index = add_error(e.to_string());
                            eprintln!("Error creating index: {:?}", e);
                            result = Err(error_index);
                        }
                    }
                }
                send_reply(reply_sender, result).await;
            }
            LanceDbCommand::Quit{ reply_sender } => {
                tables.send(TableCommand::Quit).await.unwrap();
                connections.send(ConnectionCommand::Quit).await.unwrap();
                quit_sender = Some(reply_sender);
                break;
            }
        }
    }
    println!("(RUST) Event loop shutting down.");
    if let Some(sender) = quit_sender {
        if let Err(e) = sender.send(()) {
            eprintln!("Error sending quit response: {:?}", e);
        }
    }
}

/// Open a table in the database. This function will open a table with
/// the given name, using the connection provided.
#[no_mangle]
pub extern "C" fn open_table(name: *const c_char, connection_handle: i64, schema_callback: Option<extern "C" fn(bytes: *const u8, len: u64)>) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<(TableHandle, SchemaRef), i64>>();
    if send_command(LanceDbCommand::OpenTable {
        name,
        connection_handle: ConnectionHandle(connection_handle),
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving open table response: {:?}", e);
        Err(-1)
    });
    if let Some(schema_callback) = schema_callback {
        let bytes = schema_to_bytes(&result.as_ref().unwrap().1);
        schema_callback(bytes.as_ptr(), bytes.len() as u64);
    }
    match result {
        Ok(handle) => handle.0.0,
        Err(e) => e,
    }
}

/// Drop a table from the database. This function will drop a table with
/// the given name, using the connection provided. WARNING: this invalidates
/// any cached table handles referencing the table.
#[no_mangle]
pub extern "C" fn drop_table(name: *const c_char, connection_handle: i64) -> i64 {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<(), i64>>();
    if send_command(LanceDbCommand::DropTable {
        name,
        connection_handle: ConnectionHandle(connection_handle),
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving drop table response: {:?}", e);
        Err(-1)
    });
    match result {
        Ok(_) => 0,
        Err(e) => e,
    }
}

/// Close a table
#[no_mangle]
pub extern "C" fn close_table(connection_handle: i64, table_handle: i64) -> i64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<(), i64>>();
    if send_command(LanceDbCommand::CloseTable {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving close table response: {:?}", e);
        Err(-1)
    });
    match result {
        Ok(_) => 0,
        Err(e) => e,
    }
}

/// Create a scalar index on a table
#[no_mangle]
pub extern "C" fn create_scalar_index(connection_handle: i64, table_handle: i64, column_name: *const c_char, index_type: u32, replace: bool) -> i64 {
    let column_name = unsafe { std::ffi::CStr::from_ptr(column_name).to_string_lossy().to_string() };
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<(), i64>>();
    if send_command(LanceDbCommand::CreateScalarIndex {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        column_name,
        index_type,
        replace,
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving create scalar index response: {:?}", e);
        Err(-1)
    });
    match result {
        Ok(_) => 0,
        Err(e) => e,
    }
}

/// Count the number of rows in a table
#[no_mangle]
pub extern "C" fn count_rows(connection_handle: i64, table_handle: i64) -> u64 {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<u64, i64>>();
    if send_command(LanceDbCommand::CountRows {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        reply_sender: reply_tx,
    }).is_err() {
        return 0;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving count rows response: {:?}", e);
        Err(0)
    });
    result.unwrap_or_else(|_| 0)
}

/// Add a row to a table
#[no_mangle]
pub extern "C" fn add_rows(connection_handle: i64, table_handle: i64, record_batch: *const u8, batch_len: u64) -> i64 {
    let record_batch = unsafe { std::slice::from_raw_parts(record_batch, batch_len as usize) };
    let record_batch = bytes_to_record_batch(record_batch);
    if let Err(e) = record_batch {
        let error_index = add_error(e.to_string());
        eprintln!("Error reading record batch: {:?}", e);
        return error_index;
    }
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<(), i64>>();
    if send_command(LanceDbCommand::AddRows {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        record_batch: record_batch.unwrap(),
        reply_sender: reply_tx,
    }).is_err() {
        return -1;
    }
    let result = reply_rx.blocking_recv().unwrap_or_else(|e| {
        eprintln!("Error receiving add rows response: {:?}", e);
        Err(-1)
    });
    match result {
        Ok(_) => 0,
        Err(e) => e,
    }
}