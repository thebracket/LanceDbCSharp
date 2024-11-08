use std::ffi::c_char;
use futures::TryStreamExt;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use tokio::sync::mpsc::Sender;
use crate::event_loop::connection::get_table;
use crate::event_loop::{report_result, CompletionSender, ErrorReportFn};
use crate::serialization::batch_to_bytes;
use crate::table_handler::{TableCommand, TableHandle};

pub(crate) async fn do_query(
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    batch_callback: Option<extern "C" fn(*const u8, u64)>,
    limit: Option<usize>,
    where_clause: Option<String>,
    with_row_id: bool,
    explain_callback: Option<(bool, extern "C" fn (*const c_char))>,
    selected_columns: Option<Vec<String>>,
    full_text_search: Option<String>,
) {
    let Some(table) = get_table(tables.clone(), table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return;
    };

    // Use the query builder setup
    let mut query_builder = table.query();

    // Full text search
    if let Some(query) = full_text_search {
        query_builder = query_builder.full_text_search(FullTextSearchQuery::new(query));
    }

    // Limits the number of records returned
    if let Some(limit) = limit {
        println!("Limiting query to: {}", limit);
        query_builder = query_builder.limit(limit);
    }

    // Add a where clause if one is provided
    if let Some(where_clause) = where_clause {
        query_builder = query_builder.only_if(where_clause);
    }

    // Return Row IDs
    if with_row_id {
        query_builder = query_builder.with_row_id();
    }

    // Selected columns
    if let Some(selected_columns) = selected_columns {
        query_builder = query_builder.select(Select::Columns(selected_columns));
    }

    // Explain handling
    if let Some((verbose, explain_callback)) = explain_callback {
        match query_builder.explain_plan(verbose).await {
            Err(e) => {
                let err = format!("Error explaining query: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender));
                return;
            }
            Ok(explain) => {
                let explain = format!("{:?}", explain);
                let explain = std::ffi::CString::new(explain).unwrap();
                explain_callback(explain.as_ptr());
                report_result(Ok(0), reply_tx, Some(completion_sender));
                return;
            }
        }
    }

    // Query execution (synchronous - using try_next is a better idea)
    match query_builder.execute().await {
        Ok(mut query) => {
            while let Ok(Some(record)) = query.try_next().await {
                // Return results as a batch
                if let Some(batch_callback) = batch_callback {
                    let schema = record.schema();
                    let Ok(bytes) = batch_to_bytes(&record, &schema) else {
                        report_result(Err("Unable to convert result to bytes".to_string()), reply_tx, Some(completion_sender));
                        return;
                    };
                    batch_callback(bytes.as_ptr(), bytes.len() as u64);
                }
            }

            // Announce that we're done
            report_result(Ok(0), reply_tx, Some(completion_sender));
        }
        Err(e) => {
            let err = format!("Error querying table: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender));
        }
    }
}