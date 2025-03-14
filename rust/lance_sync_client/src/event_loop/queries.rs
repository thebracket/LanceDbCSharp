use crate::connection_handler::ConnectionHandle;
use crate::event_loop::connection::get_table;
use crate::event_loop::{report_result, CompletionSender, ErrorReportFn};
use crate::serialization::{batch_to_bytes, bytes_to_batch};
use crate::table_handler::{TableCommand, TableHandle};
use arrow_array::Array;
use futures::TryStreamExt;
use half::f16;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions, Select};
use lancedb::DistanceType;
use std::ffi::c_char;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::spawn_blocking;
use crate::BlobCallback;

// Vector search data type. Holds types that accept implement VectorQuery
#[derive(Debug)]
pub enum VectorDataType {
    F16(Vec<f16>), // Note that f16 is from the `half` crate.
    F32(Vec<f32>),
    F64(Vec<f64>),
    ArrowArray(Arc<dyn Array>),
}

impl VectorDataType {
    pub(crate) fn from_blob(
        vector_type: u32,
        vector_blob: *const u8,
        vector_blob_len: u64,
        vector_num_elements: u64,
    ) -> Self {
        // Cast the blob into a vector of bytes
        let vector_blob =
            unsafe { std::slice::from_raw_parts(vector_blob, vector_blob_len as usize) };
        // Convert the blob into a vector of f32 (in memory)
        // TODO: Research - Can this be done with ZeroCopy for efficiency?
        match vector_type {
            1 => {
                // f16
                let mut vector = Vec::with_capacity(vector_num_elements as usize);
                for i in 0..vector_num_elements as usize {
                    let start = i * 2;
                    let end = start + 2;
                    let bytes = &vector_blob[start..end];
                    let f16_val = f16::from_f32(f32::from_ne_bytes([bytes[0], bytes[1], 0, 0]));
                    vector.push(f16_val);
                }
                Self::F16(vector)
            }
            2 => {
                // f32
                let mut vector = Vec::with_capacity(vector_num_elements as usize);
                for i in 0..vector_num_elements as usize {
                    let start = i * 4;
                    let end = start + 4;
                    let bytes = &vector_blob[start..end];
                    let f32_val = f32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    vector.push(f32_val);
                }
                Self::F32(vector)
            }
            3 => {
                // f64
                let mut vector = Vec::with_capacity(vector_num_elements as usize);
                for i in 0..vector_num_elements as usize {
                    let start = i * 8;
                    let end = start + 8;
                    let bytes = &vector_blob[start..end];
                    let f64_val = f64::from_ne_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    vector.push(f64_val);
                }
                Self::F64(vector)
            }
            4 => {
                // Arrow Array. This one works differently, because the whole blog
                // is one big serialized Arrow Array, in a record batch.
                // FIXME: This needs a lot of checking.
                let batch = bytes_to_batch(vector_blob);
                if let Ok(batch) = batch {
                    for entry in batch {
                        if let Ok(entry) = entry {
                            for idx in 0 .. entry.num_columns() {
                                let column = entry.column(idx);
                                return Self::ArrowArray(column.clone());
                            }
                        }
                    }
                }
                unimplemented!()
            }
            // TODO: Support Arrow Arrays. Will require deserialization.
            _ => panic!("Invalid vector type: {}", vector_type),
        }
    }
}

pub(crate) async fn do_query(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    batch_callback: BlobCallback,
    limit: Option<usize>,
    where_clause: Option<String>,
    with_row_id: bool,
    explain_callback: Option<(bool, extern "C" fn(*const c_char))>,
    selected_columns: Option<Vec<String>>,
    full_text_search: Option<String>,
    batch_size: u32,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
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
                report_result(Err(err), reply_tx, Some(completion_sender)).await;
                return;
            }
            Ok(explain) => {
                let explain = format!("{:?}", explain);
                let explain = std::ffi::CString::new(explain).unwrap();
                explain_callback(explain.as_ptr());
                report_result(Ok(0), reply_tx, Some(completion_sender)).await;
                return;
            }
        }
    }

    let options = if batch_size > 0 {
        let mut qo = QueryExecutionOptions::default();
        qo.max_batch_length = batch_size;
        qo
    } else {
        QueryExecutionOptions::default()
    };

    match query_builder.execute_with_options(options).await {
        Ok(mut query) => {
            while let Ok(Some(record)) = query.try_next().await {
                // Return results as a batch
                if let Some(batch_callback) = batch_callback {
                    let schema = record.schema();
                    let Ok(bytes) = batch_to_bytes(&record, &schema) else {
                        report_result(
                            Err("Unable to convert result to bytes".to_string()),
                            reply_tx,
                            Some(completion_sender),
                        ).await;
                        return;
                    };
                    let proceed = spawn_blocking(move || {
                        batch_callback(bytes.as_ptr(), bytes.len() as u64)
                    }).await.unwrap();
                    if !proceed {
                        break;
                    }
                }
            }

            // Announce that we're done
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        }
        Err(e) => {
            let err = format!("Error querying table: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
        }
    }
}

pub(crate) async fn do_vector_query(
    connection_handle: ConnectionHandle,
    tables: Sender<TableCommand>,
    table_handle: TableHandle,
    reply_tx: ErrorReportFn,
    completion_sender: CompletionSender,
    batch_callback: BlobCallback,
    limit: Option<usize>,
    where_clause: Option<String>,
    with_row_id: bool,
    explain_callback: Option<(bool, extern "C" fn(*const c_char))>,
    selected_columns: Option<Vec<String>>,
    vector_data: VectorDataType,
    metric: DistanceType,
    n_probes: usize,
    refine_factor: u32,
    batch_size: u32,
    distance_range_min: Option<f32>,
    distance_range_max: Option<f32>,
) {
    let Some(table) = get_table(tables.clone(), connection_handle, table_handle).await else {
        let err = format!("Table not found: {table_handle:?}");
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
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

    // Selected columns
    if let Some(selected_columns) = selected_columns {
        query_builder = query_builder.select(Select::Columns(selected_columns));
    }

    // Vector handling
    let vec_result = match vector_data {
        VectorDataType::F16(vector) => query_builder.nearest_to(vector),
        VectorDataType::F32(vector) => query_builder.nearest_to(vector),
        VectorDataType::F64(vector) => query_builder.nearest_to(vector),
        VectorDataType::ArrowArray(array) => query_builder.nearest_to(array),
    };
    if let Err(e) = vec_result {
        let err = format!("Error querying table: {:?}", e);
        report_result(Err(err), reply_tx, Some(completion_sender)).await;
        return;
    }
    let mut query_builder = vec_result.unwrap();

    // Distance metric
    query_builder = query_builder.distance_type(metric);

    // Probe count
    if n_probes > 0 {
        query_builder = query_builder.nprobes(n_probes);
    }

    if refine_factor > 0 {
        query_builder = query_builder.refine_factor(refine_factor);
    }

    if distance_range_min.is_some() || distance_range_max.is_some() {
        query_builder = query_builder.distance_range(distance_range_min, distance_range_max);
    }

    // Explain handling
    if let Some((verbose, explain_callback)) = explain_callback {
        match query_builder.explain_plan(verbose).await {
            Err(e) => {
                let err = format!("Error explaining query: {:?}", e);
                report_result(Err(err), reply_tx, Some(completion_sender)).await;
                return;
            }
            Ok(explain) => {
                let explain = format!("{:?}", explain);
                let explain = std::ffi::CString::new(explain).unwrap();
                explain_callback(explain.as_ptr());
                report_result(Ok(0), reply_tx, Some(completion_sender)).await;
                return;
            }
        }
    }

    println!("Batch size: {}", batch_size);
    let options = if batch_size > 0 {
        println!("Setting batch size to: {}", batch_size);
        let mut qo = QueryExecutionOptions::default();
        qo.max_batch_length = batch_size;
        qo
    } else {
        QueryExecutionOptions::default()
    };

    match query_builder.execute_with_options(options).await {
        Ok(mut query) => {
            while let Ok(Some(record)) = query.try_next().await {
                // Return results as a batch
                println!("Received a record from the query");
                let mut cancel = false;
                if let Some(batch_callback) = batch_callback {
                    let schema = record.schema();

                    if batch_size > 0 && record.num_rows() > batch_size as usize {
                        // Split the record into batches and yield them one by one
                        let n_slices = record.num_rows() / batch_size as usize;
                        println!("Dividing result ({} rows) into {n_slices} slices for individual yielding", record.num_rows());
                        for slice in 0..n_slices {
                            let slice = record.slice(slice * batch_size as usize, batch_size as usize);
                            let Ok(bytes) = batch_to_bytes(&slice, &schema) else {
                                report_result(
                                    Err("Unable to convert result to bytes".to_string()),
                                    reply_tx,
                                    Some(completion_sender),
                                ).await;
                                return;
                            };
                            let proceed = spawn_blocking(move || {
                                batch_callback(bytes.as_ptr(), bytes.len() as u64)
                            }).await.unwrap();
                            if !proceed {
                                cancel = true;
                                break;
                            }
                        }
                    } else {
                        // Return the whole record
                        let Ok(bytes) = batch_to_bytes(&record, &schema) else {
                            report_result(
                                Err("Unable to convert result to bytes".to_string()),
                                reply_tx,
                                Some(completion_sender),
                            ).await;
                            return;
                        };
                        let proceed = spawn_blocking(move || {
                            batch_callback(bytes.as_ptr(), bytes.len() as u64)
                        }).await.unwrap();
                        if !proceed {
                            break;
                        }
                    }
                }
                if cancel {
                    break;
                }
            } // end of while loop

            // Announce that we're done
            report_result(Ok(0), reply_tx, Some(completion_sender)).await;
        }
        Err(e) => {
            let err = format!("Error querying table: {:?}", e);
            report_result(Err(err), reply_tx, Some(completion_sender)).await;
        }
    }
}
