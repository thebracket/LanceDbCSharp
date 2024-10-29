use std::collections::HashMap;
use lancedb::{Connection, Table};
use arrow_schema::SchemaRef;
use tokio::sync::mpsc::Sender;
use crate::connection_handler::{ConnectionCommand, ConnectionHandle};
use crate::event_loop::{get_connection, report_result, CompletionSender, ErrorReportFn};

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
        handle: TableHandle,
        reply_sender: tokio::sync::oneshot::Sender<Option<Table>>,
    },
    GetTableByName {
        name: String,
        connection_handle: ConnectionHandle,
        connections: Sender<ConnectionCommand>,
        reply_sender: tokio::sync::oneshot::Sender<Result<TableHandle, String>>,
    },
    DropTable {
        name: String,
        connection_handle: ConnectionHandle,
        connections: Sender<ConnectionCommand>,
        reply_sender: ErrorReportFn,
        completion_sender: CompletionSender,
    },
    ReleaseTable { handle: TableHandle },
    Quit,
}

pub(crate) struct TableActor;

impl TableActor {
    pub(crate) async fn start() -> Sender<TableCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            let mut next_id = 1_i64;
            let mut tables = HashMap::new();

            while let Some(command) = rx.recv().await {
                match command {
                    TableCommand::AddEmptyTable { name, schema, connections, connection_handle, reply_sender, completion_sender } => {
                        let Some(cnn) = get_connection(connections.clone(), connection_handle).await else {
                            report_result(Err("Error getting connection".to_string()), reply_sender, Some(completion_sender));
                            continue;
                        };
                        let table = cnn.create_empty_table(&name, schema.into())
                            .execute()
                            .await;
                        match table {
                            Ok(t) => {
                                let new_id = next_id;
                                next_id += 1;
                                tables.insert(TableHandle(new_id), t);
                                report_result(Ok(new_id), reply_sender, Some(completion_sender));
                            }
                            Err(e) => {
                                report_result(Err(format!("Error creating table: {e:?}")), reply_sender, Some(completion_sender));
                            }
                        }
                    }
                    TableCommand::GetTable { handle, reply_sender } => {
                        if let Some(table) = tables.get(&handle) {
                            let _ = reply_sender.send(Some(table.clone()));
                        } else {
                            let _ = reply_sender.send(None);
                        }
                    }
                    TableCommand::GetTableByName { name, connection_handle, connections, reply_sender } => {
                        let Some(cnn) = get_connection(connections.clone(), connection_handle).await else {
                            let _ = reply_sender.send(Err("Error getting connection".to_string()));
                            continue;
                        };
                        let table = cnn.open_table(&name).await;
                        match table {
                            Ok(t) => {
                                let new_id = next_id;
                                next_id += 1;
                                tables.insert(TableHandle(new_id), t);
                                let _ = reply_sender.send(Ok(TableHandle(new_id)));
                            }
                            Err(e) => {
                                let err = format!("Error opening table: {e:?}");
                                let _ = reply_sender.send(Err(err));
                            }
                        }
                    }
                    TableCommand::DropTable { name, connection_handle, connections, reply_sender, completion_sender } => {
                        let Some(cnn) = get_connection(connections.clone(), connection_handle).await else {
                            report_result(Err("Error getting connection".to_string()), reply_sender, Some(completion_sender));
                            continue;
                        };
                        let result = cnn.drop_table(&name).await;
                        match result {
                            Ok(_) => {
                                report_result(Ok(0), reply_sender, Some(completion_sender));
                            }
                            Err(e) => {
                                let err = format!("Error dropping table: {e:?}");
                                report_result(Err(err), reply_sender, Some(completion_sender));
                            }
                        }
                    }
                    TableCommand::ReleaseTable { handle } => {
                        tables.remove(&handle);
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
