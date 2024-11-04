//! Module containing all the FFI exports for LanceDB.
//! These are using the C ABI and are intended to be used by other languages.

use crate::command_from_ffi;
use crate::connection_handler::ConnectionHandle;
use crate::event_loop::{report_result, ErrorReportFn, LanceDbCommand};
use crate::serialization::{bytes_to_batch, bytes_to_schema};
use crate::table_handler::TableHandle;
use std::ffi::c_char;

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
    command_from_ffi!(
        LanceDbCommand::ConnectionRequest { uri },
        "ConnectionRequest",
        reply_tx
    );
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
    command_from_ffi!(
        LanceDbCommand::Disconnect {
            handle: ConnectionHandle(handle)
        },
        "Disconnect",
        reply_tx
    );
}

/// Drop a database from the connection. This function will drop the
/// database associated with the connection handle.
#[no_mangle]
pub extern "C" fn drop_database(connection_handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(
        LanceDbCommand::DropDatabase {
            connection_handle: ConnectionHandle(connection_handle)
        },
        "DropDatabase",
        reply_tx
    );
}

/// Create a table in the database. This function will create a table
/// with the given name, using the connection and record batch provided.
#[no_mangle]
pub extern "C" fn create_empty_table(
    name: *const c_char,
    connection_handle: i64,
    schema_bytes: *const u8,
    len: usize,
    reply_tx: ErrorReportFn,
) {
    let schema_batch = unsafe { std::slice::from_raw_parts(schema_bytes, len) };
    let Ok(schema) = bytes_to_schema(schema_batch) else {
        report_result(Err("Could not process schema.".to_string()), reply_tx, None);
        return;
    };
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(
        LanceDbCommand::CreateTableWithSchema {
            name,
            connection_handle: ConnectionHandle(connection_handle),
            schema,
        },
        "CreateTableWithSchema",
        reply_tx
    );
}

/// Get a handle to a list of table names in the database.
#[no_mangle]
pub extern "C" fn list_table_names(
    connection_handle: i64,
    string_callback: Option<extern "C" fn(*const c_char)>,
    reply_tx: ErrorReportFn,
) {
    command_from_ffi!(
        LanceDbCommand::ListTableNames {
            connection_handle: ConnectionHandle(connection_handle),
            string_callback,
        },
        "ListTableNames",
        reply_tx
    );
}

/// Open a table in the database. This function will open a table with
/// the given name, using the connection provided.
#[no_mangle]
pub extern "C" fn open_table(
    name: *const c_char,
    connection_handle: i64,
    schema_callback: Option<extern "C" fn(bytes: *const u8, len: u64)>,
    reply_tx: ErrorReportFn,
) {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(
        LanceDbCommand::OpenTable {
            name,
            connection_handle: ConnectionHandle(connection_handle),
            schema_callback,
        },
        "OpenTable",
        reply_tx
    );
}

/// Drop a table from the database. This function will drop a table with
/// the given name, using the connection provided. WARNING: this invalidates
/// any cached table handles referencing the table.
#[no_mangle]
pub extern "C" fn drop_table(
    name: *const c_char,
    connection_handle: i64,
    ignore_missing: bool,
    reply_tx: ErrorReportFn,
) {
    let name = unsafe { std::ffi::CStr::from_ptr(name).to_string_lossy().to_string() };
    command_from_ffi!(
        LanceDbCommand::DropTable {
            name,
            connection_handle: ConnectionHandle(connection_handle),
            ignore_missing,
        },
        "DropTable",
        reply_tx
    );
}

/// Close a table
#[no_mangle]
pub extern "C" fn close_table(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(
        LanceDbCommand::CloseTable {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
        },
        "CloseTable",
        reply_tx
    );
}

/// Rename a table
#[no_mangle]
pub extern "C" fn rename_table(
    connection_handle: i64,
    old_name: *const c_char,
    new_name: *const c_char,
    reply_tx: ErrorReportFn,
) {
    let old_name = unsafe { std::ffi::CStr::from_ptr(old_name).to_string_lossy().to_string() };
    let new_name = unsafe { std::ffi::CStr::from_ptr(new_name).to_string_lossy().to_string() };
    command_from_ffi!(
        LanceDbCommand::RenameTable {
            connection_handle: ConnectionHandle(connection_handle),
            old_name,
            new_name,
        },
        "RenameTable",
        reply_tx
    );
}

/// Add a record batch to a table
#[no_mangle]
pub extern "C" fn add_record_batch(
    connection_handle: i64,
    table_handle: i64,
    data: *const u8,
    len: usize,
    write_mode: u32,
    bad_vector_handling: u32,
    fill_value: f32,
    reply_tx: ErrorReportFn,
) {
    let data = unsafe { std::slice::from_raw_parts(data, len) };
    let batch = bytes_to_batch(data);
    if let Err(e) = batch {
        report_result(
            Err(format!("Could not parse record batch: {:?}", e)),
            reply_tx,
            None,
        );
        return;
    }
    command_from_ffi!(
        LanceDbCommand::AddRecordBatch {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            write_mode: write_mode.into(),
            batch: batch.unwrap(),
            bad_vector_handling: bad_vector_handling.into(),
            fill_value,
        },
        "AddRecordBatch",
        reply_tx
    );
}

/// Delete rows from a table
#[no_mangle]
pub extern "C" fn delete_rows(
    connection_handle: i64,
    table_handle: i64,
    filter: *const c_char,
    reply_tx: ErrorReportFn,
) {
    let where_clause = if filter.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(filter)
                .to_string_lossy()
                .to_string()
        })
    };
    command_from_ffi!(
        LanceDbCommand::DeleteRows {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            where_clause,
        },
        "DeleteRows",
        reply_tx
    );
}

/// Create a scalar index on a table
#[no_mangle]
pub extern "C" fn create_scalar_index(
    connection_handle: i64,
    table_handle: i64,
    column_name: *const c_char,
    index_type: u32,
    replace: bool,
    reply_tx: ErrorReportFn,
) {
    let column_name = unsafe {
        std::ffi::CStr::from_ptr(column_name)
            .to_string_lossy()
            .to_string()
    };
    command_from_ffi!(
        LanceDbCommand::CreateScalarIndex {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            column_name,
            index_type: index_type.into(),
            replace,
        },
        "CreateScalarIndex",
        reply_tx
    );
}

/// Count the number of rows in a table
#[no_mangle]
pub extern "C" fn count_rows(
    connection_handle: i64,
    table_handle: i64,
    filter: *const c_char,
    reply_tx: ErrorReportFn,
) {
    let filter = if filter.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(filter)
                .to_string_lossy()
                .to_string()
        })
    };
    command_from_ffi!(
        LanceDbCommand::CountRows {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            filter,
        },
        "CountRows",
        reply_tx
    );
}

/// Compact files
#[no_mangle]
pub extern "C" fn compact_files(connection_handle: i64, table_handle: i64, reply_tx: ErrorReportFn) {
    command_from_ffi!(
        LanceDbCommand::CompactFiles {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
        },
        "CompactFiles",
        reply_tx
    );
}