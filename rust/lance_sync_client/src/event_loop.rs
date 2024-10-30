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
use crate::MAX_COMMANDS;
use std::sync::OnceLock;
use tokio::sync::mpsc::{channel, Sender};
use crate::connection_handler::{ConnectionActor, ConnectionCommand, ConnectionHandle};
use crate::event_loop::command::{get_completion_pair, LanceDbCommand};
use crate::event_loop::helpers::send_command;
use crate::table_handler::{TableActor, TableCommand, TableHandle};

pub(crate) use lifecycle::setup;
pub(crate) use errors::{ErrorReportFn, report_result};
pub(crate) use command::CompletionSender;
pub(crate) use connection::get_connection;
use crate::event_loop::connection::{do_connection_request, do_create_table_with_schema, do_disconnect, do_drop_database, do_drop_table, do_list_tables, do_open_table, get_table};

/// This static variable holds the sender for the LanceDB command.
pub(crate) static COMMAND_SENDER: OnceLock<Sender<LanceDbCommand>> = OnceLock::new();

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
            LanceDbCommand::OpenTable { name, connection_handle, reply_sender, completion_sender, schema_callback } => {
                tokio::spawn(do_open_table(tables.clone(), connections.clone(), name, connection_handle, reply_sender, completion_sender, schema_callback));
            }
            LanceDbCommand::ListTableNames { connection_handle, reply_sender, completion_sender, string_callback } => {
                tokio::spawn(do_list_tables(connections.clone(), connection_handle, reply_sender, completion_sender, string_callback));
            }
            LanceDbCommand::DropTable { name, connection_handle, reply_sender, completion_sender } => {
                tokio::spawn(do_drop_table(tables.clone(), name, connection_handle, reply_sender, completion_sender, connections.clone()));
            }
            LanceDbCommand::CloseTable { connection_handle: _, table_handle, reply_sender, completion_sender } => {
                tables.send(TableCommand::ReleaseTable { handle: table_handle }).await.unwrap();
                report_result(Ok(0), reply_sender, Some(completion_sender));
            }
            LanceDbCommand::CountRows { connection_handle, table_handle, reply_sender, completion_sender } => {
                if let Some(_cnn) = get_connection(connections.clone(), connection_handle).await {
                    if let Some(table) = get_table(tables.clone(), table_handle).await {
                        match table.count_rows(None).await {
                            Ok(count) => {
                                report_result(Ok(count as i64), reply_sender, Some(completion_sender));
                                continue;
                            }
                            Err(e) => {
                                let err = format!("Error counting rows: {:?}", e);
                                report_result(Err(err), reply_sender, Some(completion_sender));
                                continue;
                            }
                        }
                    }
                } else {
                    eprintln!("Connection handle {} not found.", connection_handle.0);
                }
                completion_sender.send(()).unwrap();
            }
            LanceDbCommand::CreateScalarIndex { connection_handle: _, table_handle, column_name, index_type: _, replace:_, reply_sender, completion_sender } => {
                if let Some(table) = get_table(tables.clone(), table_handle).await {
                    // TODO: Need to support different index types
                    match table.create_index(&[column_name], lancedb::index::Index::Auto).execute().await {
                        Ok(_) => {
                            report_result(Ok(0), reply_sender, Some(completion_sender));
                            continue;
                        }
                        Err(e) => {
                            let err = format!("Error creating index: {:?}", e);
                            report_result(Err(err), reply_sender, Some(completion_sender));
                            continue;
                        }
                    }
                }
                completion_sender.send(()).unwrap();
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

/// Close a table
#[no_mangle]
pub extern "C" fn close_table(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn) {
    let (tx, rx) = get_completion_pair();
    if send_command(LanceDbCommand::CloseTable {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        reply_sender: reply_tx,
        completion_sender: tx,
    }).is_err() {
        report_result(Err("Error sending close table request.".to_string()), reply_tx, None);
        return;
    }
    rx.blocking_recv().unwrap();
}

/// Create a scalar index on a table
#[no_mangle]
pub extern "C" fn create_scalar_index(connection_handle: i64, table_handle: i64, column_name: *const c_char, index_type: u32, replace: bool, reply_tx: ErrorReportFn) {
    let column_name = unsafe { std::ffi::CStr::from_ptr(column_name).to_string_lossy().to_string() };
    let (tx, rx) = get_completion_pair();
    if send_command(LanceDbCommand::CreateScalarIndex {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        column_name,
        index_type,
        replace,
        reply_sender: reply_tx,
        completion_sender: tx,
    }).is_err() {
        report_result(Err("Error sending drop table request.".to_string()), reply_tx, None);
        return;
    }
    rx.blocking_recv().unwrap();
}

/// Count the number of rows in a table
#[no_mangle]
pub extern "C" fn count_rows(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn){
    let (tx, rx) = get_completion_pair();
    if send_command(LanceDbCommand::CountRows {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        reply_sender: reply_tx,
        completion_sender: tx,
    }).is_err() {
        report_result(Err("Error sending drop table request.".to_string()), reply_tx, None);
        return;
    }
    rx.blocking_recv().unwrap();
}
