use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn};
use crate::serialization::schema_to_bytes;
use arrow_schema::SchemaRef;
use lancedb::Table;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tokio::task::spawn_blocking;
use crate::BlobCallback;

/// Strongly typed table handle (to disambiguate from the other handles).
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) struct TableHandle(pub(crate) i64); // Unique identifier for the connection

pub enum TableCommand {
    AddEmptyTable {
        name: String,
        schema: SchemaRef,
        connections: Sender<ConnectionCommand>,
        connection_handle: ConnectionHandle,
        reply_sender: ErrorReportFn,
        completion_sender: CompletionSender,
    },
    GetTable {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        reply_sender: tokio::sync::oneshot::Sender<Option<Table>>,
    },
    GetTableByName {
        name: String,
        connection_handle: ConnectionHandle,
        connections: Sender<ConnectionCommand>,
        reply_sender: tokio::sync::oneshot::Sender<Result<TableHandle, String>>,
        schema_callback: BlobCallback,
    },
    DropTable {
        name: String,
        connection_handle: ConnectionHandle,
        connections: Sender<ConnectionCommand>,
        reply_sender: ErrorReportFn,
        completion_sender: CompletionSender,
        ignore_missing: bool,
    },
    ReleaseTable {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
    },
    Quit,
}

pub(crate) struct TableActor;

impl TableActor {
    pub(crate) async fn start() -> Sender<TableCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            let mut next_id = 1_i64;
            // The connection is hashed with the table, to avoid reusing table objects between
            // sessions/connections.
            let mut tables = HashMap::<(ConnectionHandle, TableHandle), Table>::new();

            while let Some(command) = rx.recv().await {
                match command {
                    TableCommand::AddEmptyTable {
                        name,
                        schema,
                        connections,
                        connection_handle,
                        reply_sender,
                        completion_sender,
                    } => {
                        let Some(cnn) =
                            get_connection(connections.clone(), connection_handle).await
                        else {
                            report_result(
                                Err("Error getting connection".to_string()),
                                reply_sender,
                                Some(completion_sender),
                            ).await;
                            continue;
                        };
                        let table = cnn.create_empty_table(&name, schema.into()).execute().await;
                        match table {
                            Ok(t) => {
                                let new_id = next_id;
                                next_id += 1;
                                tables.insert((connection_handle, TableHandle(new_id)), t);
                                report_result(Ok(new_id), reply_sender, Some(completion_sender)).await;
                            }
                            Err(e) => {
                                report_result(
                                    Err(format!("Error creating table: {e:?}")),
                                    reply_sender,
                                    Some(completion_sender),
                                ).await;
                            }
                        }
                    }
                    TableCommand::GetTable {
                        connection_handle,
                        table_handle,
                        reply_sender,
                    } => {
                        if let Some(table) = tables.get(&(connection_handle, table_handle)) {
                            if let Err(e) = table.checkout_latest().await {
                                println!("Error checking out table: {e:?}");
                            }
                            let _ = reply_sender.send(Some(table.clone()));
                        } else {
                            let _ = reply_sender.send(None);
                        }
                    }
                    TableCommand::GetTableByName {
                        name,
                        connection_handle,
                        connections,
                        reply_sender,
                        schema_callback,
                    } => {
                        println!("Getting table by name: {name}");
                        let Some(cnn) =
                            get_connection(connections.clone(), connection_handle).await
                        else {
                            let _ = reply_sender.send(Err("Error getting connection".to_string()));
                            continue;
                        };
                        let table = cnn.open_table(&name).execute().await;
                        println!("Got table by name: {name}");
                        match table {
                            Ok(t) => {
                                if let Some(cb) = schema_callback {
                                    let schema = t.schema().await.unwrap();
                                    let schema_bytes = schema_to_bytes(&schema);
                                    let _ = spawn_blocking(move || {
                                        cb(schema_bytes.as_ptr(), schema_bytes.len() as u64);
                                    }).await;
                                }

                                let new_id = next_id;
                                next_id += 1;
                                tables.insert((connection_handle, TableHandle(new_id)), t);

                                let _ = reply_sender.send(Ok(TableHandle(new_id)));
                            }
                            Err(e) => {
                                let err = format!("Error opening table: {e:?}");
                                let _ = reply_sender.send(Err(err));
                            }
                        }
                    }
                    TableCommand::DropTable {
                        name,
                        connection_handle,
                        connections,
                        reply_sender,
                        completion_sender,
                        ignore_missing,
                    } => {
                        let Some(cnn) =
                            get_connection(connections.clone(), connection_handle).await
                        else {
                            report_result(
                                Err("Error getting connection".to_string()),
                                reply_sender,
                                Some(completion_sender),
                            ).await;
                            continue;
                        };
                        let result = cnn.drop_table(&name).await;
                        match result {
                            Ok(_) => {
                                report_result(Ok(0), reply_sender, Some(completion_sender)).await;
                            }
                            Err(e) => {
                                if ignore_missing {
                                    report_result(Ok(0), reply_sender, Some(completion_sender)).await;
                                } else {
                                    let err = format!("Error dropping table: {e:?}");
                                    report_result(Err(err), reply_sender, Some(completion_sender)).await;
                                }
                            }
                        }
                    }
                    TableCommand::ReleaseTable {
                        connection_handle,
                        table_handle,
                    } => {
                        tables.remove(&(connection_handle, table_handle));
                    }
                    TableCommand::Quit => {
                        break;
                    }
                }
            }
        });
        tx
    }
}
