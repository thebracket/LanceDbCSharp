use crate::event_loop::{report_result, CompletionSender, ErrorReportFn};
use lancedb::{connect, Connection};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

/// Strong type to wrap an i64 as a connection handle.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ConnectionHandle(pub(crate) i64); // Unique identifier for the connection

pub(crate) enum ConnectionCommand {
    NewConnection {
        uri: String,
        reply_sender: ErrorReportFn,
        completion_sender: CompletionSender,
    },
    Disconnect {
        handle: ConnectionHandle,
        reply_sender: ErrorReportFn,
        completion_sender: CompletionSender,
    },
    GetConnection {
        handle: ConnectionHandle,
        reply_sender: tokio::sync::oneshot::Sender<Option<Connection>>,
    },
    Quit,
}

pub struct ConnectionActor {}

impl ConnectionActor {
    pub async fn start() -> Sender<ConnectionCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            let mut next_handle = 1_i64;
            let mut connections = HashMap::new();

            while let Some(command) = rx.recv().await {
                match command {
                    ConnectionCommand::NewConnection {
                        uri,
                        reply_sender,
                        completion_sender,
                    } => {
                        let connection = connect(&uri).execute().await;
                        match connection {
                            Ok(cnn) => {
                                let new_handle_id = next_handle;
                                next_handle += 1;
                                connections.insert(new_handle_id, cnn);
                                report_result(
                                    Ok(new_handle_id),
                                    reply_sender,
                                    Some(completion_sender),
                                );
                            }
                            Err(e) => {
                                report_result(
                                    Err(format!("Error acquiring connection: {e:?}")),
                                    reply_sender,
                                    Some(completion_sender),
                                );
                            }
                        }
                    }
                    ConnectionCommand::Disconnect {
                        handle,
                        reply_sender,
                        completion_sender,
                    } => {
                        if let Some(_connection) = connections.remove(&handle.0) {
                            report_result(Ok(0), reply_sender, Some(completion_sender));
                        } else {
                            report_result(
                                Err("Connection not found".to_string()),
                                reply_sender,
                                Some(completion_sender),
                            );
                        }
                    }
                    ConnectionCommand::GetConnection {
                        handle,
                        reply_sender,
                    } => {
                        let connection = connections.get(&handle.0).cloned();
                        let _ = reply_sender.send(connection);
                    }
                    ConnectionCommand::Quit => break,
                }
            }
        });
        tx
    }
}
