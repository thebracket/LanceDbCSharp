use std::ffi::c_char;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
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
) {
    let Some(table) = get_table(tables.clone(), table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender));
        return;
    };

    let Ok(schema) = table.schema().await else {
        report_result(
            Err("Could not get schema for table.".to_string()),
            reply_tx,
            Some(completion_sender),
        );
        return;
    };

    // Use the query builder setup
    let mut query_builder = table.query();

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

    // Explain handling
    println!("Explain callback: {:?}", explain_callback);
    if let Some((verbose, explain_callback)) = explain_callback {
        println!("Explaining query");
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

    // Query execution
    match query_builder.execute().await {
        Ok(query) => {
            // We have the result - need to transmit it back to the caller
            let records = query.try_collect::<Vec<_>>()
                .await;
            let Ok(records) = records else {
                report_result(Err("Error collecting query results".to_string()), reply_tx, Some(completion_sender));
                return;
            };
            for record in records.iter() {
                if let Some(batch_callback) = batch_callback {
                    let Ok(bytes) = batch_to_bytes(record, &schema) else {
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