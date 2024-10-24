use std::sync::Arc;
use crate::batch_handler::RecBatch;
use crate::connection_handle::ConnectionHandle;

/// Commands that can be sent to the LanceDB event-loop.
#[derive(Debug)]
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

    /// Simple "nearest" query.
    QueryNearest {
        limit: u64,
        vector: Vec<f32>,
        table_handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Request to remove a binary blob from memory.
    FreeBlob {
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<i64>,
    },

    /// Query the length of a blob
    BlobLen {
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<Option<isize>>,
    },

    /// Get Blob Pointer
    GetBlobPointer {
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<Option<Arc<Vec<u8>>>>,
    },

    /// Gracefully shut down the event-loop.
    Quit,
}