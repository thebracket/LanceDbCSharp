//! Bridging two different async environments together can be
//! tricky. One way that works consistently is to have Rust manage
//! its async, and C# (etc.) manage their own - and provide a bridge
//! through a message-passing interface.

pub(crate) mod command;
mod connection;
mod errors;
pub(crate) mod helpers;
mod lifecycle;
mod merge_insert;
mod metric;
mod queries;
mod table;

use crate::connection_handler::{ConnectionActor, ConnectionCommand};
use crate::table_handler::{TableActor, TableCommand};
use crate::MAX_COMMANDS;
pub(crate) use command::LanceDbCommand;
use std::sync::OnceLock;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};

use crate::event_loop::connection::{
    do_connection_request, do_create_table_with_schema, do_disconnect, do_drop_database,
    do_drop_table, do_list_tables, do_open_table, do_rename_table,
};
pub(crate) use command::CompletionSender;
pub(crate) use connection::get_connection;
pub(crate) use errors::{report_result, report_result_sync, ErrorReportFn};
pub(crate) use lifecycle::setup;
pub(crate) use metric::MetricType;
pub(crate) use queries::VectorDataType;

/// This static variable holds the sender for the LanceDB command.
pub(crate) static COMMAND_SENDER: OnceLock<Sender<LanceDbCommandSet>> = OnceLock::new();

pub(crate) struct LanceDbCommandSet {
    /// The command to execute.
    pub(crate) command: LanceDbCommand,
    /// Function pointer to report the result back to the caller.
    reply_tx: ErrorReportFn,
    /// Function pointer to report completion back to the caller.
    pub(crate) completion_sender: CompletionSender,
}

async fn event_loop(ready_tx: tokio::sync::oneshot::Sender<Handle>) {
    let (tx, mut rx) = channel::<LanceDbCommandSet>(MAX_COMMANDS);
    if let Err(e) = COMMAND_SENDER.set(tx) {
        eprintln!("Error setting up command sender: {:?}", e);
        return;
    }

    // Create a connection factory to handle mapping handles to connections
    let connections = ConnectionActor::start().await;

    // Table handler
    let tables = TableActor::start().await;

    // Signal readiness
    let tokio_handle = Handle::current();
    ready_tx.send(tokio_handle).unwrap();

    let mut quit_sender = None;
    while let Some(command) = rx.recv().await {
        // Extract the components of the command
        let LanceDbCommandSet {
            command,
            reply_tx,
            completion_sender,
        } = command;

        // Match on the command itself
        match command {
            LanceDbCommand::ConnectionRequest { uri, storage_options } => {
                tokio::spawn(do_connection_request(
                    connections.clone(),
                    uri,
                    reply_tx,
                    completion_sender,
                    storage_options,
                ));
            }
            LanceDbCommand::Disconnect { handle } => {
                tokio::spawn(do_disconnect(
                    connections.clone(),
                    handle,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::DropDatabase { connection_handle } => {
                tokio::spawn(do_drop_database(
                    connections.clone(),
                    connection_handle,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::CreateTableWithSchema {
                name,
                connection_handle,
                schema,
            } => {
                tokio::spawn(do_create_table_with_schema(
                    connections.clone(),
                    tables.clone(),
                    connection_handle,
                    name,
                    schema,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::OpenTable {
                name,
                connection_handle,
                schema_callback,
            } => {
                tokio::spawn(do_open_table(
                    tables.clone(),
                    connections.clone(),
                    name,
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    schema_callback,
                ));
            }
            LanceDbCommand::ListTableNames {
                connection_handle,
                string_callback,
            } => {
                tokio::spawn(do_list_tables(
                    connections.clone(),
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    string_callback,
                ));
            }
            LanceDbCommand::DropTable {
                name,
                connection_handle,
                ignore_missing,
            } => {
                tokio::spawn(do_drop_table(
                    tables.clone(),
                    name,
                    connection_handle,
                    reply_tx,
                    completion_sender,
                    connections.clone(),
                    ignore_missing,
                ));
            }
            LanceDbCommand::RenameTable {
                connection_handle,
                old_name,
                new_name,
            } => {
                tokio::spawn(do_rename_table(
                    connection_handle,
                    connections.clone(),
                    old_name,
                    new_name,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::CloseTable {
                connection_handle,
                table_handle,
            } => {
                tables
                    .send(TableCommand::ReleaseTable {
                        connection_handle,
                        table_handle,
                    })
                    .await
                    .unwrap();
                report_result(Ok(0), reply_tx, Some(completion_sender)).await;
            }
            LanceDbCommand::AddRecordBatch {
                connection_handle,
                table_handle,
                write_mode,
                batch,
            } => {
                tokio::spawn(table::do_add_record_batch(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    write_mode,
                    batch,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::MergeInsert {
                connection_handle,
                table_handle,
                columns,
                when_not_matched_insert_all,
                where_clause,
                when_not_matched_by_source_delete,
                batch,
            } => {
                tokio::spawn(merge_insert::do_merge_insert_with_record_batch(
                    connection_handle,
                    table_handle,
                    tables.clone(),
                    columns.unwrap_or_default(),
                    when_not_matched_insert_all,
                    where_clause,
                    when_not_matched_by_source_delete,
                    batch,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::CountRows {
                connection_handle,
                table_handle,
                filter,
            } => {
                tokio::spawn(table::do_count_rows(
                    connections.clone(),
                    tables.clone(),
                    connection_handle,
                    table_handle,
                    filter,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::DeleteRows {
                connection_handle,
                table_handle,
                where_clause,
            } => {
                tokio::spawn(table::do_delete_rows(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    where_clause.unwrap_or("".to_string()),
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::Query {
                connection_handle,
                table_handle,
                batch_callback,
                limit,
                where_clause,
                with_row_id,
                explain_callback,
                selected_columns,
                full_text_search,
                batch_size,
            } => {
                tokio::spawn(queries::do_query(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    batch_callback,
                    limit,
                    where_clause,
                    with_row_id,
                    explain_callback,
                    selected_columns,
                    full_text_search,
                    batch_size,
                ));
            }
            LanceDbCommand::VectorQuery {
                connection_handle,
                table_handle,
                batch_callback,
                limit,
                where_clause,
                with_row_id,
                explain_callback,
                selected_columns,
                vector_data,
                metric,
                n_probes,
                refine_factor,
                batch_size,
            } => {
                tokio::spawn(queries::do_vector_query(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    batch_callback,
                    limit,
                    where_clause,
                    with_row_id,
                    explain_callback,
                    selected_columns,
                    vector_data,
                    metric,
                    n_probes,
                    refine_factor,
                    batch_size,
                ));
            }
            LanceDbCommand::CreateScalarIndex {
                connection_handle,
                table_handle,
                column_name,
                index_type,
                replace,
            } => {
                tokio::spawn(table::do_crate_scalar_index(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    column_name,
                    index_type,
                    replace,
                    reply_tx,
                    completion_sender,
                ));
            }
            LanceDbCommand::CreateIndex {
                connection_handle,
                table_handle,
                column_name,
                metric,
                num_partitions,
                num_sub_vectors,
                replace,
            } => {
                tokio::spawn(table::do_create_index(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    column_name,
                    metric,
                    num_partitions,
                    num_sub_vectors,
                    replace,
                ));
            }
            LanceDbCommand::CreateFullTextIndex {
                connection_handle,
                table_handle,
                columns,
                with_position,
                replace,
                tokenizer_name,
            } => {
                tokio::spawn(table::do_add_fts_index(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    columns,
                    with_position,
                    replace,
                    tokenizer_name,
                ));
            }
            LanceDbCommand::OptimizeTable {
                connection_handle,
                table_handle,
                prune_older_than,
                delete_unverified,
                compaction_callback,
                prune_callback,
            } => {
                tokio::spawn(table::do_optimize_table(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    prune_older_than,
                    delete_unverified,
                    reply_tx,
                    completion_sender,
                    compaction_callback,
                    prune_callback,
                ));
            }
            LanceDbCommand::Update {
                connection_handle,
                table_handle,
                updates,
                where_clause,
                update_callback,
            } => {
                tokio::spawn(table::do_update(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    where_clause,
                    updates,
                    update_callback,
                ));
            }
            LanceDbCommand::ListIndices { connection_handle, table_handle, string_callback } => {
                tokio::spawn(table::do_list_table_indices(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    reply_tx,
                    completion_sender,
                    string_callback,
                ));
            }
            LanceDbCommand::GetIndexStats { connection_handle, table_handle, index_name, callback } => {
                tokio::spawn(table::do_get_index_stats(
                    connection_handle,
                    tables.clone(),
                    table_handle,
                    index_name,
                    reply_tx,
                    completion_sender,
                    callback,
                ));
            }
            LanceDbCommand::Quit { reply_sender } => {
                tables.send(TableCommand::Quit).await.unwrap();
                connections.send(ConnectionCommand::Quit).await.unwrap();
                quit_sender = Some(reply_sender);
                break;
            }
        }
    }
    println!("(RUST) Event loop shutting down.");
    if let Some(sender) = quit_sender {
        if let Err(e) = sender.send(()) {
            eprintln!("Error sending quit response: {:?}", e);
        }
    }
}
