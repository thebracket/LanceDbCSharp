use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::ArrowError;
use lancedb::index::Index;
use lancedb::index::scalar::{BTreeIndexBuilder, BitmapIndexBuilder, LabelListIndexBuilder};
use tokio::sync::mpsc::Sender;
use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn};
use crate::event_loop::command::{BadVectorHandling, IndexType, WriteMode};
use crate::event_loop::connection::get_table;
use crate::table_handler::{TableCommand, TableHandle};

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
        report_result(Err("Error getting table schema".to_string()), reply_tx, Some(completion_sender));
        return;
    };
    let batch = RecordBatchIterator::new(batch, schema);
    let result = table.add(batch)
        .mode(write_mode.into())
        .execute()
        .await;

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
            },
            IndexType::Bitmap => {
                let builder = BitmapIndexBuilder::default();
                table.create_index(&[column_name], Index::Bitmap(builder))
            }
            IndexType::LabelList => {
                let builder = LabelListIndexBuilder::default();
                table.create_index(&[column_name], Index::LabelList(builder))
            }
        };

        match build_command
            .replace(replace)
            .execute()
            .await
        {
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