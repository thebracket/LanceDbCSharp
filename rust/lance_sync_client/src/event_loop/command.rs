use crate::batch_handler::RecBatch;

/// Commands that can be sent to the LanceDB event-loop.
pub(crate) enum LanceDbCommand {
    /// Request to create a new connection to the database.
    ConnectionRequest{
        uri: String,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Request to disconnect a connection from the database.
    Disconnect{
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Request to send a record batch to the database. The data will be
    /// de-serialized and made available via a handle.
    SendRecordBatch {
        batch: RecBatch,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Request to free a record batch from the database. Already having
    /// been freed is not an error.
    FreeRecordBatch {
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Create a table in the database.
    CreateTable {
        name: String,
        connection_handle: i64,
        record_batch_handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Gracefully shut down the event-loop.
    Quit,
}