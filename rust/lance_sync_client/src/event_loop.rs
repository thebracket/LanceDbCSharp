//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

pub(crate) mod command;
mod connection;
mod errors;
pub(crate) mod helpers;
mod lifecycle;

use crate::connection_handler::{ConnectionActor, ConnectionCommand};
use crate::table_handler::{TableActor, TableCommand};
use crate::MAX_COMMANDS;
pub(crate) use command::LanceDbCommand;
use std::sync::OnceLock;
use tokio::sync::mpsc::{channel, Sender};

use crate::event_loop::connection::{
    do_connection_request, do_create_table_with_schema, do_disconnect, do_drop_database,
    do_drop_table, do_list_tables, do_open_table, get_table,
};
pub(crate) use command::CompletionSender;
pub(crate) use connection::get_connection;
pub(crate) use errors::{report_result, ErrorReportFn};
pub(crate) use lifecycle::setup;

/// This static variable holds the sender for the LanceDB command.
pub(crate) static COMMAND_SENDER: OnceLock<Sender<LanceDbCommandSet>> = OnceLock::new();

pub(crate) struct LanceDbCommandSet {
    /// The command to execute.
    pub(crate) command: LanceDbCommand,
    /// Function pointer to report the result back to the caller.
    reply_tx: ErrorReportFn,
    /// Function pointer to report completion back to the caller.
    pub(crate) completion_sender: CompletionSender,
}

async fn event_loop(ready_tx: tokio::sync::oneshot::Sender<()>) {
    let (tx, mut rx) = channel::<LanceDbCommandSet>(MAX_COMMANDS);
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
        // Extract the components of the command
        let LanceDbCommandSet {
            command,
            reply_tx,
            completion_sender,
        } = command;

        // Match on the command itself
        match command {
            LanceDbCommand::ConnectionRequest { uri } => {
                tokio::spawn(do_connection_request(
                    connections.clone(),
                    uri,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::Disconnect { handle } => {
                tokio::spawn(do_disconnect(
                    connections.clone(),
                    handle,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::DropDatabase { connection_handle } => {
                tokio::spawn(do_drop_database(
                    connections.clone(),
                    connection_handle,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::CreateTableWithSchema {
                name,
                connection_handle,
                schema,
            } => {
                tokio::spawn(do_create_table_with_schema(
                    connections.clone(),
                    tables.clone(),
                    connection_handle,
                    name,
                    schema,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::OpenTable {
                name,
                connection_handle,
                schema_callback,
            } => {
                tokio::spawn(do_open_table(
                    tables.clone(),
                    connections.clone(),
                    name,
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    schema_callback,
                ));
            }
            LanceDbCommand::ListTableNames {
                connection_handle,
                string_callback,
            } => {
                tokio::spawn(do_list_tables(
                    connections.clone(),
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    string_callback,
                ));
            }
            LanceDbCommand::DropTable {
                name,
                connection_handle,
            } => {
                tokio::spawn(do_drop_table(
                    tables.clone(),
                    name,
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    connections.clone(),
                ));
            }
            LanceDbCommand::CloseTable {
                connection_handle: _,
                table_handle,
            } => {
                tables
                    .send(TableCommand::ReleaseTable {
                        handle: table_handle,
                    })
                    .await
                    .unwrap();
                report_result(Ok(0), reply_tx, Some(completion_sender));
            }
            LanceDbCommand::CountRows {
                connection_handle,
                table_handle,
            } => {
                if let Some(_cnn) = get_connection(connections.clone(), connection_handle).await {
                    if let Some(table) = get_table(tables.clone(), table_handle).await {
                        match table.count_rows(None).await {
                            Ok(count) => {
                                report_result(Ok(count as i64), reply_tx, Some(completion_sender));
                                continue;
                            }
                            Err(e) => {
                                let err = format!("Error counting rows: {:?}", e);
                                report_result(Err(err), reply_tx, Some(completion_sender));
                                continue;
                            }
                        }
                    }
                } else {
                    eprintln!("Connection handle {} not found.", connection_handle.0);
                }
                completion_sender.send(()).unwrap();
            }
            LanceDbCommand::CreateScalarIndex {
                connection_handle: _,
                table_handle,
                column_name,
                index_type: _,
                replace: _,
            } => {
                if let Some(table) = get_table(tables.clone(), table_handle).await {
                    // TODO: Need to support different index types
                    match table
                        .create_index(&[column_name], lancedb::index::Index::Auto)
                        .execute()
                        .await
                    {
                        Ok(_) => {
                            report_result(Ok(0), reply_tx, Some(completion_sender));
                            continue;
                        }
                        Err(e) => {
                            let err = format!("Error creating index: {:?}", e);
                            report_result(Err(err), reply_tx, Some(completion_sender));
                            continue;
                        }
                    }
                }
                completion_sender.send(()).unwrap();
            }
            LanceDbCommand::Quit { reply_sender } => {
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
