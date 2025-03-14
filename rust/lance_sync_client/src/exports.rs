//! Module containing all the FFI exports for LanceDB.
//! These are using the C ABI and are intended to be used by other languages.

use crate::command_from_ffi;
use crate::connection_handler::ConnectionHandle;
use crate::event_loop::{report_result_sync, ErrorReportFn, LanceDbCommand, MetricType, VectorDataType};
use crate::serialization::{bytes_to_batch, bytes_to_schema};
use crate::table_handler::TableHandle;
use std::ffi::c_char;
use crate::event_loop::command::{ScalarIndexType, WriteMode};

/// Defines a function type for a "blob" callback: a bunch of bytes and a length.
pub type BlobCallback = Option<extern "C" fn(bytes: *const u8, len: u64) -> bool>;

/// Connect to a LanceDB database. This function will return a handle
/// to the connection, which can be used in other functions.
///
/// Parameters:
/// - `uri`: The URI to connect to.
/// - `options_length`: The number of options in the `options` array. Must be an even number.
/// - `options`: An array of strings representing options for the connection.
///
/// Return values:
/// - A handle to the connection, or -1 if an error occurred.
#[no_mangle]
pub extern "C" fn connect(uri: *const c_char, options_length: u64, options: *const *const c_char, reply_tx: ErrorReportFn) {
    if options_length % 2 != 0 {
        report_result_sync(Err("Options length must be an even number, representing key/value pairs.".to_string()), reply_tx, None);
        return;
    }
    let storage_options = if options_length == 0 {
        None
    } else {
        let mut storage_options = Vec::new();
        for i in 0..options_length/2 {
            let base = (i * 2) as isize;
            let key = unsafe {
                std::ffi::CStr::from_ptr(*options.offset(base))
                    .to_string_lossy()
                    .to_string()
            };
            let value = unsafe {
                std::ffi::CStr::from_ptr(*options.offset(base+1))
                    .to_string_lossy()
                    .to_string()
            };
            storage_options.push((key, value));
        }
        Some(storage_options)
    };

    let uri = unsafe { std::ffi::CStr::from_ptr(uri).to_string_lossy().to_string() };
    let uri = uri.replace("file://", ""); // LanceDb really doesn't like file:// prefixes
    command_from_ffi!(
        LanceDbCommand::ConnectionRequest { uri, storage_options },
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
        report_result_sync(Err("Could not process schema.".to_string()), reply_tx, None);
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
    schema_callback: BlobCallback,
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
    let old_name = unsafe {
        std::ffi::CStr::from_ptr(old_name)
            .to_string_lossy()
            .to_string()
    };
    let new_name = unsafe {
        std::ffi::CStr::from_ptr(new_name)
            .to_string_lossy()
            .to_string()
    };
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
    reply_tx: ErrorReportFn,
) {
    let data = unsafe { std::slice::from_raw_parts(data, len) };
    let batch = bytes_to_batch(data);
    if let Err(e) = batch {
        report_result_sync(
            Err(format!("Could not parse record batch: {:?}", e)),
            reply_tx,
            None,
        );
        return;
    }
    let Some(write_mode) = WriteMode::from_repr(write_mode) else {
        report_result_sync(Err("Invalid write mode.".to_string()), reply_tx, None);
        return;
    };
    command_from_ffi!(
        LanceDbCommand::AddRecordBatch {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            write_mode,
            batch: batch.unwrap(),
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
    let Some(index_type) = ScalarIndexType::from_repr(index_type) else {
        report_result_sync(Err("Invalid index type.".to_string()), reply_tx, None);
        return;
    };
    command_from_ffi!(
        LanceDbCommand::CreateScalarIndex {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            column_name,
            index_type,
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

/// Create an index
#[no_mangle]
pub extern "C" fn create_index(
    connection_handle: i64,
    table_handle: i64,
    column_name: *const c_char,
    metric: u32,
    num_partitions: u32,
    num_sub_vectors: u32,
    replace: bool,
    reply_tx: ErrorReportFn,
) {
    let column_name = unsafe {
        std::ffi::CStr::from_ptr(column_name)
            .to_string_lossy()
            .to_string()
    };
    let Some(metric) = MetricType::from_repr(metric) else {
        report_result_sync(Err("Invalid metric.".to_string()), reply_tx, None);
        return;
    };
    command_from_ffi!(
        LanceDbCommand::CreateIndex {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            column_name,
            metric: metric.into(),
            num_partitions,
            num_sub_vectors,
            replace,
        },
        "CreateIndex",
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
    prune_older_than_seconds: i64, // negative means "none"
    delete_unverified: bool,
    reply_tx: ErrorReportFn,
    compaction_callback: extern "C" fn(u64, u64, u64, u64),
    prune_callback: extern "C" fn(u64, u64),
) {
    command_from_ffi!(
        LanceDbCommand::OptimizeTable {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            prune_older_than: if prune_older_than_seconds < 0 {
                None
            } else {
                Some(chrono::Duration::from_std(std::time::Duration::from_secs(prune_older_than_seconds as u64)).unwrap())
            },
            delete_unverified,
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
    batch_callback: BlobCallback,
    reply_tx: ErrorReportFn,
    limit: u64,
    where_clause: *const c_char,
    with_row_id: bool,
    selected_columns: *const *const c_char,
    selected_columns_len: u64,
    full_text_search: *const c_char,
    batch_size: u32,
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
            limit: if limit == 0 {
                None
            } else {
                Some(limit as usize)
            },
            where_clause,
            with_row_id,
            explain_callback: None,
            selected_columns,
            full_text_search,
            batch_size,
        },
        "Query",
        reply_tx
    );
}

/// Initial query code
#[no_mangle]
pub extern "C" fn vector_query(
    connection_handle: i64,
    table_handle: i64,
    batch_callback: BlobCallback,
    reply_tx: ErrorReportFn,
    limit: u64,
    where_clause: *const c_char,
    with_row_id: bool,
    selected_columns: *const *const c_char,
    selected_columns_len: u64,
    vector_type: u32,
    vector_blob: *const u8,
    vector_blob_len: u64,
    vector_num_elements: u64,
    metric: u32,
    n_probes: u64,
    refine_factor: u32,
    batch_size: u32,
    distance_range_min: f32,
    distance_range_max: f32,
) {
    let Some(metric) = MetricType::from_repr(metric) else {
        report_result_sync(Err("Invalid metric.".to_string()), reply_tx, None);
        return;
    };
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

    let vector_data = VectorDataType::from_blob(
        vector_type,
        vector_blob,
        vector_blob_len,
        vector_num_elements,
    );

    command_from_ffi!(
        LanceDbCommand::VectorQuery {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            batch_callback,
            limit: if limit == 0 {
                None
            } else {
                Some(limit as usize)
            },
            where_clause,
            with_row_id,
            explain_callback: None,
            selected_columns,
            vector_data,
            metric: metric.into(),
            n_probes: n_probes as usize,
            refine_factor,
            batch_size,
            distance_range_min: if distance_range_min.is_nan() {
                None
            } else {
                Some(distance_range_min)
            },
            distance_range_max: if distance_range_max.is_nan() {
                None
            } else {
                Some(distance_range_max)
            },
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
            limit: if limit == 0 {
                None
            } else {
                Some(limit as usize)
            },
            where_clause,
            with_row_id,
            explain_callback: Some((verbose, explain_callback)),
            selected_columns,
            full_text_search,
            batch_size: 0,
        },
        "ExplainQuery",
        reply_tx
    );
}

/// Explain a vector query
#[no_mangle]
pub extern "C" fn explain_vector_query(
    connection_handle: i64,
    table_handle: i64,
    reply_tx: ErrorReportFn,
    limit: u64,
    where_clause: *const c_char,
    with_row_id: bool,
    verbose: bool,
    explain_callback: extern "C" fn(*const c_char),
    selected_columns: *const *const c_char,
    selected_columns_len: u64,
    vector_type: u32,
    vector_blob: *const u8,
    vector_blob_len: u64,
    vector_num_elements: u64,
    metric: u32,
    n_probes: u64,
    refine_factor: u32,
    distance_range_min: f32,
    distance_range_max: f32,
) {
    let Some(metric) = MetricType::from_repr(metric) else {
        report_result_sync(Err("Invalid metric.".to_string()), reply_tx, None);
        return;
    };
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

    let vector_data = VectorDataType::from_blob(
        vector_type,
        vector_blob,
        vector_blob_len,
        vector_num_elements,
    );

    command_from_ffi!(
        LanceDbCommand::VectorQuery {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            batch_callback: None,
            limit: if limit == 0 {
                None
            } else {
                Some(limit as usize)
            },
            where_clause,
            with_row_id,
            explain_callback: Some((verbose, explain_callback)),
            selected_columns,
            vector_data,
            metric: metric.into(),
            n_probes: n_probes as usize,
            refine_factor,
            batch_size: 0,
            distance_range_min: if distance_range_min.is_nan() {
                None
            } else {
                Some(distance_range_min)
            },
            distance_range_max: if distance_range_max.is_nan() {
                None
            } else {
                Some(distance_range_max)
            },
        },
        "Query",
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

    let where_clause = if where_clause.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(where_clause)
                .to_string_lossy()
                .to_string()
        })
    };

    let when_not_matched_by_source_delete = if when_not_matched_by_source_delete.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(when_not_matched_by_source_delete)
                .to_string_lossy()
                .to_string()
        })
    };

    let data = unsafe { std::slice::from_raw_parts(batch, batch_len) };
    let batch = bytes_to_batch(data);
    if let Err(e) = batch {
        report_result_sync(
            Err(format!("Could not parse record batch: {:?}", e)),
            reply_tx,
            None,
        );
        return;
    }
    let batch = batch.unwrap();
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

/// Update rows in a table
#[no_mangle]
pub extern "C" fn update_rows(
    connection_handle: i64,
    table_handle: i64,
    updates: *const *const c_char,
    updates_len: u64,
    where_clause: *const c_char,
    reply_tx: ErrorReportFn,
    callback: Option<extern "C" fn(u64)>,
) {
    let mut update_list: Vec<(String, String)> = Vec::new();
    for i in 0..updates_len {
        let update = unsafe {
            std::ffi::CStr::from_ptr(*updates.offset(i as isize))
                .to_string_lossy()
                .to_string()
        };
        let parts: Vec<&str> = update.split('=').collect();
        if parts.len() != 2 {
            report_result_sync(Err("Invalid update statement".to_string()), reply_tx, None);
            return;
        }
        update_list.push((parts[0].to_string(), parts[1].to_string()));
    }

    let where_clause = if where_clause.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(where_clause)
                .to_string_lossy()
                .to_string()
        })
    };

    command_from_ffi!(
        LanceDbCommand::Update {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            updates: update_list,
            where_clause,
            update_callback: callback,
        },
        "Update",
        reply_tx
    );
}

/// List indices in a table
#[no_mangle]
pub extern "C" fn list_indices(
    connection_handle: i64,
    table_handle: i64,
    string_callback: Option<extern "C" fn(*const c_char, u32, *const *const c_char, column_count: u64)>,
    reply_tx: ErrorReportFn,
) {
    command_from_ffi!(
        LanceDbCommand::ListIndices {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            string_callback,
        },
        "ListIndices",
        reply_tx
    );
}

/// Get index statistics
#[no_mangle]
pub extern "C" fn get_index_statistics(
    connection_handle: i64,
    table_handle: i64,
    index_name: *const c_char,
    callback: Option<extern "C" fn(u32, u32, u64, u64, u64)>,
    reply_tx: ErrorReportFn,
) {
    let index_name = unsafe {
        std::ffi::CStr::from_ptr(index_name)
            .to_string_lossy()
            .to_string()
    };
    command_from_ffi!(
        LanceDbCommand::GetIndexStats {
            connection_handle: ConnectionHandle(connection_handle),
            table_handle: TableHandle(table_handle),
            index_name,
            callback,
        },
        "GetIndexStatistics",
        reply_tx
    );
}