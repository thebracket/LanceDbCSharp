use std::ffi::{c_char, CString};
use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::command::{IndexType, ScalarIndexType, WriteMode};
use crate::event_loop::connection::get_table;
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn, MetricType};
use crate::table_handler::{TableCommand, TableHandle};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::ArrowError;
use lancedb::index::scalar::{
    BTreeIndexBuilder, BitmapIndexBuilder, FtsIndexBuilder, LabelListIndexBuilder,
};
use lancedb::index::Index;
use lancedb::table::{OptimizeAction, OptimizeOptions};
use lancedb::DistanceType;
use tokio::sync::mpsc::Sender;

pub(crate) async fn do_count_rows(
    connections: Sender<ConnectionCommand>,
    tables: Sender<TableCommand>,
    connection_handle: ConnectionHandle,
    table_handle: TableHandle,
    filter: Option<String>,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(_cnn) = get_connection(connections.clone(), connection_handle).await {
        let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
            let err = format!("Table not found: {table_handle:?}");
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        };
        match table.count_rows(filter).await {
            Ok(count) => {
                report_result(Ok(count as i64), reply_tx, Some(completion_sender)).await;
                return;
            }
            Err(e) => {
                let err = format!("Error counting rows: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender)).await;
                return;
            }
        }
    } else {
        eprintln!("Connection handle {} not found.", connection_handle.0);
        report_result(Err("Connection not found".to_string()), reply_tx, Some(completion_sender)).await;
    }
}

pub(crate) async fn do_add_record_batch(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    write_mode: WriteMode,
    batch: Vec<Result<RecordBatch, ArrowError>>,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    // TODO: Implement bad vector handling
    // TODO: Implement fill value

    let Ok(schema) = table.schema().await else {
        report_result(
            Err("Error getting table schema".to_string()),
            reply_tx,
            Some(completion_sender),
        ).await;
        return;
    };
    let batch = RecordBatchIterator::new(batch, schema);
    let result = table.add(batch).mode(write_mode.into()).execute().await;

    match result {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        }
        Err(e) => {
            let err = format!("Error adding record batch: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
        }
    }
}

pub(crate) async fn do_delete_rows(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    where_clause: String,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };
    match table.delete(&where_clause).await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        }
        Err(e) => {
            let err = format!("Error deleting rows: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
        }
    }
}

pub(crate) async fn do_crate_scalar_index(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    column_name: String,
    index_type: ScalarIndexType,
    replace: bool,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };
    let build_command = match index_type {
        ScalarIndexType::BTree => {
            let builder = BTreeIndexBuilder::default();
            table.create_index(&[column_name], Index::BTree(builder))
        }
        ScalarIndexType::Bitmap => {
            let builder = BitmapIndexBuilder::default();
            table.create_index(&[column_name], Index::Bitmap(builder))
        }
        ScalarIndexType::LabelList => {
            let builder = LabelListIndexBuilder::default();
            table.create_index(&[column_name], Index::LabelList(builder))
        }
    };

    match build_command.replace(replace).execute().await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
            return;
        }
        Err(e) => {
            let err = format!("Error creating index: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }
    }
}

pub(crate) async fn do_create_index(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    column_name: String,
    metric: DistanceType,
    num_partitions: u32,
    num_sub_vectors: u32,
    replace: bool,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    let mut idx_builder = lancedb::index::vector::IvfPqIndexBuilder::default();
    idx_builder = idx_builder.distance_type(metric);
    idx_builder = idx_builder.num_partitions(num_partitions);
    idx_builder = idx_builder.num_sub_vectors(num_sub_vectors);
    let build_command = table
        .create_index(&[column_name], Index::IvfPq(idx_builder))
        .replace(replace);

    match build_command.replace(replace).execute().await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
            return;
        }
        Err(e) => {
            let err = format!("Error creating index: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }
    }
}

pub(crate) async fn do_add_fts_index(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    columns: Vec<String>,
    with_position: bool,
    replace: bool,
    tokenizer_name: String,
) {
    //TODO: Where are the other options? OrderingColumns, tantivvy, etc.?
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    let mut fts_builder = FtsIndexBuilder::default();
    fts_builder = fts_builder.with_position(with_position);

    // Now the hard part: filling the tokenizer configs
    if !(tokenizer_name.is_empty() || tokenizer_name == "default") {
        fts_builder.tokenizer_configs = fts_builder.tokenizer_configs.base_tokenizer(tokenizer_name);
    }

    let columns = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();
    let mut index_builder = table.create_index(&columns, Index::FTS(fts_builder));
    index_builder = index_builder.replace(replace);

    match index_builder.execute().await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        }
        Err(e) => {
            let err = format!("Error creating FTS index: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
        }
    }
}

pub(crate) async fn do_optimize_table(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    prune_older_than: Option<chrono::Duration>,
    delete_unverified: bool,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    compaction_callback: extern "C" fn(u64, u64, u64, u64),
    prune_callback: extern "C" fn(u64, u64),
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    if prune_older_than.is_some() || delete_unverified {
        let optimize = table.optimize(OptimizeAction::Prune {
            older_than: prune_older_than,
            delete_unverified: Some(delete_unverified),
            error_if_tagged_old_versions: None,
        }).await;

        // Run this as a two-step process, optimize and then compact
        if let Err(e) = optimize {
            let err = format!("Error pruning table: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }
        let compact = table.optimize(OptimizeAction::Compact { options: Default::default(), remap_options: None }).await;
        if let Err(e) = compact {
            let err = format!("Error compacting table: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }

        // We know they are good, so unwrap is ok
        let optimize_stats = optimize.unwrap();
        let compact_stats = compact.unwrap();

        if let Some(stats) = optimize_stats.prune {
            prune_callback(stats.bytes_removed, stats.old_versions);
        }
        if let Some(stats) = compact_stats.compaction {
            compaction_callback(
                stats.files_added as u64,
                stats.files_removed as u64,
                stats.fragments_added as u64,
                stats.fragments_removed as u64,
            );
        }

        // Index Optimization
        if let Err(e) = table.optimize(OptimizeAction::Index(OptimizeOptions::default())).await {
            let err = format!("Error optimizing indices: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }

        // Complete
        report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        return;
    }

    // No options - do it all
    match table.optimize(OptimizeAction::All).await {
        Ok(stats) => {
            if let Some(stats) = stats.compaction {
                compaction_callback(
                    stats.files_added as u64,
                    stats.files_removed as u64,
                    stats.fragments_added as u64,
                    stats.fragments_removed as u64,
                );
            }
            if let Some(stats) = stats.prune {
                prune_callback(stats.bytes_removed, stats.old_versions);
            }
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
            return;
        }
        Err(e) => {
            let err = format!("Error compacting files: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
            return;
        }
    }
}

pub(crate) async fn do_update(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    where_clause: Option<String>,
    updates: Vec<(String, String)>,
    update_callback: Option<extern "C" fn(u64)>,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    for (column, data) in updates {
        let mut update_builder = table.update();
        if let Some(where_clause) = &where_clause {
            update_builder = update_builder.only_if(where_clause);
        }
        update_builder = update_builder.column(column, data);
        match update_builder.execute().await {
            Ok(num_rows) => {
                if let Some(callback) = update_callback {
                    callback(num_rows);
                }
            }
            Err(e) => {
                let err = format!("Error updating table: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender)).await;
                return;
            }
        }
    }

    // Indicate that it is done
    completion_sender.send((0, String::new())).unwrap();
}

pub(crate) async fn do_list_table_indices(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    index_callback: Option<extern "C" fn(*const c_char, u32, *const *const c_char, column_count: u64)>,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    let Ok(indices) = table.list_indices().await else {
        report_result(
            Err("Error listing table indices".to_string()),
            reply_tx,
            Some(completion_sender),
        ).await;
        return;
    };

    for index in indices {
        let index_type_ffi: IndexType = index.index_type.into();
        let index_index:u32 = index_type_ffi as u32;
        let index_name = CString::new(index.name).unwrap().into_raw();
        let index_columns = index.
            columns.
            iter().
            map(|c| CString::new(c.as_str()).unwrap().into_raw() as *const c_char).
            collect::<Vec<*const c_char>>();
        if let Some(index_callback) = index_callback {
            index_callback(index_name, index_index, index_columns.as_ptr(), index.columns.len() as u64);
        }
    }


    // Indicate that it is done
    completion_sender.send((0, String::new())).unwrap();
}

pub(crate) async fn do_get_index_stats(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    index_name: String,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    // fn(index_type_u32, metric_type_u32, num_indexed_rows: u64, num_indices: u64, num_index_rows: u64)
    callback: Option<extern "C" fn(u32, u32, u64, u64, u64)>,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    };

    match table.index_stats(&index_name).await {
        Ok(Some(stats)) => {
            let index_type_ffi: IndexType = stats.index_type.into();
            let index_index:u32 = index_type_ffi as u32;
            let metric_type_ffi: MetricType = if let Some(metric) = stats.distance_type {
                metric.into()
            } else {
                MetricType::None
            };
            let metric_index:u32 = metric_type_ffi as u32;
            let num_indexed_rows = stats.num_indexed_rows as u64;
            let num_indices = stats.num_indices.unwrap_or(0) as u64;
            let num_unindex_rows = stats.num_unindexed_rows as u64;
            if let Some(callback) = callback {
                callback(index_index, metric_index, num_indexed_rows, num_indices, num_unindex_rows);
            }
        }
        Ok(None) => {
            if let Some(callback) = callback {
                callback(0, 0, 0, 0, 0);
            }
        }
        Err(e) => {
            let err = format!("Error getting index stats: {:?}", e);
            report_result(
                Err(err),
                reply_tx,
                Some(completion_sender),
            ).await;
            return;
        }
    }

    // Indicate that it is done
    completion_sender.send((0, String::new())).unwrap();
}