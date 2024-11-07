use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::command::{BadVectorHandling, IndexType, WriteMode};
use crate::event_loop::connection::get_table;
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn};
use crate::table_handler::{TableCommand, TableHandle};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::ArrowError;
use lancedb::index::scalar::{BTreeIndexBuilder, BitmapIndexBuilder, FtsIndexBuilder, LabelListIndexBuilder};
use lancedb::index::{Index, IndexBuilder};
use lancedb::table::OptimizeAction;
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
        if let Some(table) = get_table(tables.clone(), table_handle).await {
            match table.count_rows(filter).await {
                Ok(count) => {
                    report_result(Ok(count as i64), reply_tx, Some(completion_sender));
                    return;
                }
                Err(e) => {
                    let err = format!("Error counting rows: {:?}", e);
                    report_result(Err(err), reply_tx, Some(completion_sender));
                    return;
                }
            }
        }
    } else {
        eprintln!("Connection handle {} not found.", connection_handle.0);
    }
    completion_sender.send(()).unwrap();
}

pub(crate) async fn do_add_record_batch(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    write_mode: WriteMode,
    batch: Vec<Result<RecordBatch, ArrowError>>,
    bad_vector_handling: BadVectorHandling,
    fill_value: f32,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let Some(table) = get_table(tables.clone(), table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return;
    };

    // TODO: Implement bad vector handling
    // TODO: Implement fill value

    let Ok(schema) = table.schema().await else {
        report_result(
            Err("Error getting table schema".to_string()),
            reply_tx,
            Some(completion_sender),
        );
        return;
    };
    let batch = RecordBatchIterator::new(batch, schema);
    let result = table.add(batch).mode(write_mode.into()).execute().await;

    match result {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender));
        }
        Err(e) => {
            let err = format!("Error adding record batch: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender));
        }
    }
}

pub(crate) async fn do_delete_rows(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    where_clause: String,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    let Some(table) = get_table(tables.clone(), table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return;
    };
    match table.delete(&where_clause).await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender));
        }
        Err(e) => {
            let err = format!("Error deleting rows: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender));
        }
    }

}

pub(crate) async fn do_crate_scalar_index(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    column_name: String,
    index_type: IndexType,
    replace: bool,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(table) = get_table(tables.clone(), table_handle).await {
        let build_command = match index_type {
            IndexType::BTree => {
                let builder = BTreeIndexBuilder::default();
                table.create_index(&[column_name], Index::BTree(builder))
            }
            IndexType::Bitmap => {
                let builder = BitmapIndexBuilder::default();
                table.create_index(&[column_name], Index::Bitmap(builder))
            }
            IndexType::LabelList => {
                let builder = LabelListIndexBuilder::default();
                table.create_index(&[column_name], Index::LabelList(builder))
            }
        };

        match build_command.replace(replace).execute().await {
            Ok(_) => {
                report_result(Ok(0), reply_tx, Some(completion_sender));
                return;
            }
            Err(e) => {
                let err = format!("Error creating index: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender));
                return;
            }
        }
    }
    completion_sender.send(()).unwrap();
}

pub(crate) async fn do_add_fts_index(
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
    let Some(table) = get_table(tables.clone(), table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return;
    };

    let mut fts_builder = FtsIndexBuilder::default();
    fts_builder = fts_builder.with_position(with_position);

    // Now the hard part: filling the tokenizer configs
    // TODO: Is this right? It looks like "simple" is actually the default
    let tokenizer_name = if tokenizer_name.is_empty() || tokenizer_name == "default" {
        "simple".to_string()
    } else {
        tokenizer_name
    };
    fts_builder.tokenizer_configs = fts_builder.tokenizer_configs.base_tokenizer(tokenizer_name);

    let columns = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();
    let mut index_builder = table.create_index(&columns, Index::FTS(fts_builder));
    index_builder = index_builder.replace(replace);

    match index_builder.execute().await {
        Ok(_) => {
            report_result(Ok(0), reply_tx, Some(completion_sender));
        }
        Err(e) => {
            let err = format!("Error creating FTS index: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender));
        }
    }
}

pub(crate) async fn do_optimize_table(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    compaction_callback: extern "C" fn(u64, u64, u64, u64),
    prune_callback: extern "C" fn(u64, u64),
) {
    if let Some(table) = get_table(tables.clone(), table_handle).await {
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
                report_result(Ok(0), reply_tx, Some(completion_sender));
                return;
            }
            Err(e) => {
                let err = format!("Error compacting files: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender));
                return;
            }
        }
    }
    completion_sender.send(()).unwrap();
}