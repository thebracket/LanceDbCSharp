//! Provides support for the merge-insert idiom.

use anyhow::Result;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::ArrowError;
use tokio::sync::mpsc::Sender;
use crate::connection_handler::ConnectionHandle;
use crate::event_loop::{report_result, CompletionSender, ErrorReportFn};
use crate::event_loop::connection::get_table;
use crate::table_handler::{TableCommand, TableHandle};

pub(crate) async fn do_merge_insert_with_record_batch(
    connection_handle: ConnectionHandle,
    table_handle: TableHandle,
    table_actor: Sender<TableCommand>,
    columns: Vec<String>,
    when_not_matched_insert_all: bool,
    where_clause: Option<String>,
    when_not_matched_by_source_delete: Option<String>,
    batch: Vec<std::result::Result<RecordBatch, ArrowError>>,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) -> Result<()> {
    let Some(table) = get_table(table_actor.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return Ok(());
    };
    let columns = columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
    let mut merge_insert_builder = table.merge_insert(&columns);

    merge_insert_builder.when_matched_update_all(where_clause);

    if when_not_matched_insert_all {
        merge_insert_builder.when_not_matched_insert_all();
    }
    if when_not_matched_by_source_delete.is_some() {
        merge_insert_builder.when_not_matched_by_source_delete(when_not_matched_by_source_delete);
    }
    let schema = table.schema().await?;

    // Execute
    let batch = Box::new(RecordBatchIterator::new(batch, schema));
    merge_insert_builder.execute(batch).await?;

    report_result(Ok(0), reply_tx, Some(completion_sender));

    Ok(())
}