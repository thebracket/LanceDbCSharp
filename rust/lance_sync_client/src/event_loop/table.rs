use tokio::sync::mpsc::Sender;
use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn};
use crate::event_loop::connection::get_table;
use crate::table_handler::{TableCommand, TableHandle};

pub(crate) async fn count_rows(
    connections: Sender<ConnectionCommand>,
    tables: Sender<TableCommand>,
    connection_handle: ConnectionHandle,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(_cnn) = get_connection(connections.clone(), connection_handle).await {
        if let Some(table) = get_table(tables.clone(), table_handle).await {
            match table.count_rows(None).await {
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

pub(crate) async fn crate_scalar_index(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    column_name: String,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
) {
    if let Some(table) = get_table(tables.clone(), table_handle).await {
        // TODO: Need to support different index types
        match table
            .create_index(&[column_name], lancedb::index::Index::Auto)
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