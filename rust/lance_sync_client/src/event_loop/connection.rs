use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::errors::{report_result, ErrorReportFn};
use crate::event_loop::CompletionSender;
use crate::table_handler::{TableCommand, TableHandle};
use arrow_schema::SchemaRef;
use lancedb::{Connection, Table};
use std::ffi::c_char;
use tokio::sync::mpsc::Sender;

pub(crate) async fn get_connection(
    connections: Sender<ConnectionCommand>,
    handle: ConnectionHandle,
) -> Option<Connection> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = connections
        .send(ConnectionCommand::GetConnection {
            handle,
            reply_sender: tx,
        })
        .await;
    rx.await.unwrap()
}

pub(crate) async fn get_table(tables: Sender<TableCommand>, handle: TableHandle) -> Option<Table> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = tables
        .send(TableCommand::GetTable {
            handle,
            reply_sender: tx,
        })
        .await;
    rx.await.unwrap()
}

pub(crate) async fn do_connection_request(
    connections: Sender<ConnectionCommand>,
    uri: String,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let _ = connections
        .send(ConnectionCommand::NewConnection {
            uri,
            reply_sender,
            completion_sender,
        })
        .await;
}

pub(crate) async fn do_disconnect(
    connections: Sender<ConnectionCommand>,
    handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let _ = connections
        .send(ConnectionCommand::Disconnect {
            handle,
            reply_sender,
            completion_sender,
        })
        .await;
}

pub(crate) async fn do_drop_database(
    connections: Sender<ConnectionCommand>,
    connection_handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(cnn) = get_connection(connections.clone(), connection_handle).await {
        match cnn.drop_db().await {
            Ok(_) => {
                report_result(Ok(0), reply_sender, Some(completion_sender));
            }
            Err(e) => {
                let error = format!("Error dropping database: {:?}", e);
                report_result(Err(error), reply_sender, Some(completion_sender));
            }
        }
    } else {
        let error = format!("Connection handle {} not found.", connection_handle.0);
        report_result(Err(error), reply_sender, Some(completion_sender));
    }
}

pub(crate) async fn do_list_tables(
    connections: Sender<ConnectionCommand>,
    connection_handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
    string_callback: Option<extern "C" fn(*const c_char)>,
) {
    if let Some(cnn) = get_connection(connections, connection_handle).await {
        match cnn.table_names().execute().await {
            Ok(tables) => {
                for t in tables.iter() {
                    if let Some(cb) = string_callback {
                        let table_name = std::ffi::CString::new(t.clone()).unwrap();
                        cb(table_name.as_ptr());
                    }
                }
                report_result(Ok(0), reply_sender, Some(completion_sender));
            }
            Err(e) => {
                let err = format!("Error listing table names: {:?}", e);
                report_result(Err(err), reply_sender, Some(completion_sender));
            }
        }
    } else {
        let err = format!("Connection handle {} not found.", connection_handle.0);
        report_result(Err(err), reply_sender, Some(completion_sender));
    }
}

pub(crate) async fn do_create_table_with_schema(
    connections: Sender<ConnectionCommand>,
    tables: Sender<TableCommand>,
    connection_handle: ConnectionHandle,
    name: String,
    schema: SchemaRef,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(_cnn) = get_connection(connections.clone(), connection_handle).await {
        tables
            .send(TableCommand::AddEmptyTable {
                name: name.clone(),
                schema: schema.clone(),
                connections: connections.clone(),
                connection_handle,
                reply_sender: reply_sender.clone(),
                completion_sender,
            })
            .await
            .unwrap();
    }
}

pub(crate) async fn do_open_table(
    tables: Sender<TableCommand>,
    connections: Sender<ConnectionCommand>,
    name: String,
    connection_handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
    schema_callback: Option<extern "C" fn(bytes: *const u8, len: u64)>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tables
        .send(TableCommand::GetTableByName {
            name,
            connection_handle,
            connections: connections.clone(),
            reply_sender: tx,
            schema_callback,
        })
        .await
        .unwrap();
    match rx.await {
        Ok(Ok(handle)) => {
            let _ = report_result(Ok(handle.0), reply_sender, Some(completion_sender));
        }
        Ok(Err(e)) => {
            let err = format!("Error opening table: {:?}", e);
            let _ = report_result(Err(err), reply_sender, Some(completion_sender));
        }
        Err(e) => {
            let err = format!("Error receiving table handle: {:?}", e);
            let _ = report_result(Err(err), reply_sender, Some(completion_sender));
        }
    }
}

pub(crate) async fn do_drop_table(
    tables: Sender<TableCommand>,
    name: String,
    connection_handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
    connections: Sender<ConnectionCommand>,
    ignore_missing: bool,
) {
    if tables
        .send(TableCommand::DropTable {
            name,
            connection_handle,
            connections: connections.clone(),
            reply_sender,
            completion_sender,
            ignore_missing,
        })
        .await
        .is_err()
    {
        report_result(
            Err("Error sending drop table request.".to_string()),
            reply_sender,
            None,
        );
        return;
    }
}
