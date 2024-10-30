use std::ffi::c_char;
use arrow_schema::SchemaRef;
use lancedb::{Connection, Table};
use tokio::sync::mpsc::Sender;
use crate::command_from_ffi;
use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::CompletionSender;
use crate::event_loop::errors::{report_result, ErrorReportFn};
use crate::event_loop::helpers::send_command;
use crate::serialization::bytes_to_schema;
use crate::table_handler::{TableCommand, TableHandle};

/// Connect to a LanceDB database. This function will return a handle
/// to the connection, which can be used in other functions.
///
/// Parameters:
/// - `uri`: The URI to connect to.
///
/// Return values:
/// - A handle to the connection, or -1 if an error occurred.
#[no_mangle]
pub extern "C" fn connect(uri: *const c_char, reply_tx: ErrorReportFn) {
    let uri = unsafe { std::ffi::CStr::from_ptr(uri).to_string_lossy().to_string() };
    command_from_ffi!(LanceDbCommand::ConnectionRequest { uri }, "ConnectionRequest", reply_tx);
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
pub extern "C" fn disconnect(handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(LanceDbCommand::Disconnect { handle: ConnectionHandle(handle) }, "Disconnect", reply_tx);
}

/// Drop a database from the connection. This function will drop the
/// database associated with the connection handle.
#[no_mangle]
pub extern "C" fn drop_database(connection_handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(LanceDbCommand::DropDatabase { connection_handle: ConnectionHandle(connection_handle) }, "DropDatabase", reply_tx);
}

/// Create a table in the database. This function will create a table
/// with the given name, using the connection and record batch provided.
#[no_mangle]
pub extern "C" fn create_empty_table(name: *const c_char, connection_handle: i64, schema_bytes: *const u8, len: usize, reply_tx: ErrorReportFn) {
    let schema_batch = unsafe { std::slice::from_raw_parts(schema_bytes, len) };
    let Ok(schema) = bytes_to_schema(schema_batch) else {
        report_result(Err("Could not process schema.".to_string()), reply_tx, None);
        return;
    };
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(LanceDbCommand::CreateTableWithSchema {
        name,
        connection_handle: ConnectionHandle(connection_handle),
        schema,
    }, "CreateTableWithSchema", reply_tx);
}

/// Get a handle to a list of table names in the database.
#[no_mangle]
pub extern "C" fn list_table_names(connection_handle: i64, string_callback: Option<extern "C" fn(*const c_char)>, reply_tx: ErrorReportFn) {
    command_from_ffi!(LanceDbCommand::ListTableNames {
        connection_handle: ConnectionHandle(connection_handle),
        string_callback,
    }, "ListTableNames", reply_tx);
}

/// Open a table in the database. This function will open a table with
/// the given name, using the connection provided.
#[no_mangle]
pub extern "C" fn open_table(name: *const c_char, connection_handle: i64, schema_callback: Option<extern "C" fn(bytes: *const u8, len: u64)>, reply_tx: ErrorReportFn) {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(LanceDbCommand::OpenTable {
        name,
        connection_handle: ConnectionHandle(connection_handle),
        schema_callback,
    }, "OpenTable", reply_tx);
}

/// Drop a table from the database. This function will drop a table with
/// the given name, using the connection provided. WARNING: this invalidates
/// any cached table handles referencing the table.
#[no_mangle]
pub extern "C" fn drop_table(name: *const c_char, connection_handle: i64, reply_tx: ErrorReportFn) {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(LanceDbCommand::DropTable {
        name,
        connection_handle: ConnectionHandle(connection_handle),
    }, "DropTable", reply_tx);
}

pub(crate) async fn get_connection(
    connections: Sender<ConnectionCommand>,
    handle: ConnectionHandle,
) -> Option<Connection> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = connections.send(ConnectionCommand::GetConnection {
        handle,
        reply_sender: tx,
    }).await;
    rx.await.unwrap()
}

pub(crate) async fn get_table(
    tables: Sender<TableCommand>,
    handle: TableHandle,
) -> Option<Table> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = tables.send(TableCommand::GetTable {
        handle,
        reply_sender: tx,
    }).await;
    rx.await.unwrap()
}

pub(crate) async fn do_connection_request(
    connections: Sender<ConnectionCommand>,
    uri: String,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let _ = connections.send(ConnectionCommand::NewConnection {
        uri,
        reply_sender,
        completion_sender,
    }).await;
}

pub(crate) async fn do_disconnect(
    connections: Sender<ConnectionCommand>,
    handle: ConnectionHandle,
    reply_sender: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let _ = connections.send(ConnectionCommand::Disconnect {
        handle,
        reply_sender,
        completion_sender,
    }).await;
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
    string_callback: Option<extern "C" fn(*const c_char)>
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
        tables.send(TableCommand::AddEmptyTable {
            name: name.clone(),
            schema: schema.clone(),
            connections: connections.clone(),
            connection_handle,
            reply_sender: reply_sender.clone(),
            completion_sender,
        }).await.unwrap();
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
    tables.send(TableCommand::GetTableByName {
        name,
        connection_handle,
        connections: connections.clone(),
        reply_sender: tx,
        schema_callback,
    }).await.unwrap();
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
) {
    if tables.send(TableCommand::DropTable {
        name,
        connection_handle,
        connections: connections.clone(),
        reply_sender,
        completion_sender,
    }).await.is_err() {
        report_result(Err("Error sending drop table request.".to_string()), reply_sender, None);
        return;
    }
}