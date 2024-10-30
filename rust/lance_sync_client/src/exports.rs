//! Module containing all the FFI exports for LanceDB.
//! These are using the C ABI and are intended to be used by other languages.

use std::ffi::c_char;
use crate::command_from_ffi;
use crate::connection_handler::ConnectionHandle;
use crate::event_loop::{report_result, ErrorReportFn, LanceDbCommand};
use crate::serialization::bytes_to_schema;
use crate::table_handler::TableHandle;

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

/// Close a table
#[no_mangle]
pub extern "C" fn close_table(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(LanceDbCommand::CloseTable {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
    }, "CloseTable", reply_tx);
}

/// Create a scalar index on a table
#[no_mangle]
pub extern "C" fn create_scalar_index(connection_handle: i64, table_handle: i64, column_name: *const c_char, index_type: u32, replace: bool, reply_tx: ErrorReportFn) {
    let column_name = unsafe { std::ffi::CStr::from_ptr(column_name).to_string_lossy().to_string() };
    command_from_ffi!(LanceDbCommand::CreateScalarIndex {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
        column_name,
        index_type,
        replace,
    }, "CreateScalarIndex", reply_tx);
}

/// Count the number of rows in a table
#[no_mangle]
pub extern "C" fn count_rows(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn){
    command_from_ffi!(LanceDbCommand::CountRows {
        connection_handle: ConnectionHandle(connection_handle),
        table_handle: TableHandle(table_handle),
    }, "CountRows", reply_tx);
}
