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

/// Create full text index
#[no_mangle]
pub extern "C" fn create_full_text_index(
    connection_handle: i64,
    table_handle: i64,
    columns: *const *const c_char,
    columns_len: u64,
    with_position: bool,
    replace: bool,
    tokenizer_name: *const c_char,
    reply_tx: ErrorReportFn,
) {
    let columns = if columns.is_null() {
        None
    } else {
        let mut columns_list = Vec::new();
        for i in 0..columns_len {
            let column = unsafe {
                std::ffi::CStr::from_ptr(*columns.offset(i as isize))
                    .to_string_lossy()
                    .to_string()
            };
            columns_list.push(column);
        }
        Some(columns_list)
    };
    let tokenizer_name = unsafe {
        std::ffi::CStr::from_ptr(tokenizer_name)
            .to_string_lossy()
            .to_string()
    };
    command_from_ffi!(
        LanceDbCommand::CreateFullTextIndex {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            columns: columns.unwrap_or_default(),
            with_position,
            replace,
            tokenizer_name,
        },
        "CreateFullTextIndex",
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
pub extern "C" fn optimize_table(
    connection_handle: i64,
    table_handle: i64,
    reply_tx: ErrorReportFn,
    compaction_callback: extern "C" fn(u64, u64, u64, u64),
    prune_callback: extern "C" fn(u64, u64),
) {
    command_from_ffi!(
        LanceDbCommand::OptimizeTable {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            compaction_callback,
            prune_callback,
        },
        "CompactFiles",
        reply_tx
    );
}

/// Initial query code
#[no_mangle]
pub extern "C" fn query(
    connection_handle: i64,
    table_handle: i64,
    batch_callback: Option<extern "C" fn(*const u8, u64)>,
    reply_tx: ErrorReportFn,
    limit: u64,
    where_clause: *const c_char,
    with_row_id: bool,
    selected_columns: *const *const c_char,
    selected_columns_len: u64,
    full_text_search: *const c_char,
) {
    let where_clause = if where_clause.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(where_clause)
                .to_string_lossy()
                .to_string()
        })
    };

    // Selected columns - C array of strings
    let selected_columns = if selected_columns.is_null() {
        None
    } else {
        let mut columns = Vec::new();
        for i in 0..selected_columns_len {
            let column = unsafe {
                std::ffi::CStr::from_ptr(*selected_columns.offset(i as isize))
                    .to_string_lossy()
                    .to_string()
            };
            columns.push(column);
        }
        Some(columns)
    };

    let full_text_search = if full_text_search.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(full_text_search)
                .to_string_lossy()
                .to_string()
        })
    };

    command_from_ffi!(
        LanceDbCommand::Query {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            batch_callback,
            limit: if limit == 0 { None } else { Some(limit as usize) },
            where_clause,
            with_row_id,
            explain_callback: None,
            selected_columns,
            full_text_search,
        },
        "Query",
        reply_tx
    );
}

/// Explain a query
#[no_mangle]
pub extern "C" fn explain_query(
    connection_handle: i64,
    table_handle: i64,
    limit: u64,
    where_clause: *const c_char,
    with_row_id: bool,
    verbose: bool,
    explain_callback: extern "C" fn(*const c_char),
    reply_tx: ErrorReportFn,
    selected_columns: *const *const c_char,
    selected_columns_len: u64,
    full_text_search: *const c_char,
) {
    let where_clause = if where_clause.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(where_clause)
                .to_string_lossy()
                .to_string()
        })
    };

    // Selected columns - C array of strings
    let selected_columns = if selected_columns.is_null() {
        None
    } else {
        let mut columns = Vec::new();
        for i in 0..selected_columns_len {
            let column = unsafe {
                std::ffi::CStr::from_ptr(*selected_columns.offset(i as isize))
                    .to_string_lossy()
                    .to_string()
            };
            columns.push(column);
        }
        Some(columns)
    };

    let full_text_search = if full_text_search.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(full_text_search)
                .to_string_lossy()
                .to_string()
        })
    };

    command_from_ffi!(
        LanceDbCommand::Query {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            batch_callback: None,
            limit: if limit == 0 { None } else { Some(limit as usize) },
            where_clause,
            with_row_id,
            explain_callback: Some((verbose, explain_callback)),
            selected_columns,
            full_text_search,
        },
        "ExplainQuery",
        reply_tx
    );
}

/// MergeInsert with a record batch
#[no_mangle]
pub extern "C" fn merge_insert_with_record_batch(
    connection_handle: i64,
    table_handle: i64,
    columns: *const *const c_char,
    columns_len: u64,
    when_not_matched_insert_all: bool,
    where_clause: *const c_char,
    when_not_matched_by_source_delete: *const c_char,
    batch: *const u8,
    batch_len: usize,
    reply_tx: ErrorReportFn,
) {
    println!("RUNNING: merge_insert_with_record_batch");
    let columns: Option<Vec<String>> = if columns.is_null() {
        None
    } else {
        let mut column_list: Vec<String> = Vec::new();
        for i in 0..columns_len {
            let column = unsafe {
                std::ffi::CStr::from_ptr(*columns.offset(i as isize))
                    .to_string_lossy()
                    .to_string()
            };
            column_list.push(column);
        }
        Some(column_list)
    };
    println!("COLUMNS DONE");

    let where_clause = if where_clause.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(where_clause)
                .to_string_lossy()
                .to_string()
        })
    };
    println!("WHERE DONE");

    let when_not_matched_by_source_delete = if when_not_matched_by_source_delete.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(when_not_matched_by_source_delete)
                .to_string_lossy()
                .to_string()
        })
    };
    println!("WHEN DONE");

    let data = unsafe { std::slice::from_raw_parts(batch, batch_len) };
    println!("DATA DONE");
    let batch = bytes_to_batch(data);
    println!("BATCH DONE");
    if let Err(e) = batch {
        println!("Calling report_result {:?}", e);
        report_result(
            Err(format!("Could not parse record batch: {:?}", e)),
            reply_tx,
            None,
        );
        return;
    }
    let batch = batch.unwrap();
    println!("merge_insert_with_record_batch submitting");
    command_from_ffi!(
        LanceDbCommand::MergeInsert {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            columns,
            when_not_matched_insert_all,
            where_clause,
            when_not_matched_by_source_delete,
            batch,
        },
        "MergeInsert",
        reply_tx
    );
}