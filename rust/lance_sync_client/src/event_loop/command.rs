use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use crate::batch_handler::{RecBatch, RecordBatchHandle};
use crate::blob_handler::BlobHandle;
use crate::connection_handle::ConnectionHandle;
use crate::table_handler::TableHandle;

/// Commands that can be sent to the LanceDB event-loop.
#[derive(Debug)]
pub(crate) enum LanceDbCommand {
    /// Request to create a new connection to the database.
    ConnectionRequest{
        uri: String,
        reply_sender: tokio::sync::oneshot::Sender<Result<ConnectionHandle, i64>>,
    },

    /// Request to disconnect a connection from the database.
    Disconnect{
        handle: i64,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Request to send a record batch to the database. The data will be
    /// de-serialized and made available via a handle.
    SendRecordBatch {
        batch: RecBatch,
        reply_sender: tokio::sync::oneshot::Sender<Result<RecordBatchHandle, i64>>,
    },

    /// Request to free a record batch from the database. Already having
    /// been freed is not an error.
    FreeRecordBatch {
        handle: RecordBatchHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Create a table in the database with initial data.
    CreateTableWithData {
        name: String,
        connection_handle: ConnectionHandle,
        schema: SchemaRef,
        record_batch: Vec<Result<RecordBatch, ArrowError>>,
        reply_sender: tokio::sync::oneshot::Sender<Result<TableHandle, i64>>,
    },

    CreateTableWithSchema {
        name: String,
        connection_handle: ConnectionHandle,
        schema: SchemaRef,
        reply_sender: tokio::sync::oneshot::Sender<Result<TableHandle, i64>>,
    },

    /// Open a table in the database.
    OpenTable {
        name: String,
        connection_handle: ConnectionHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(TableHandle, SchemaRef), i64>>,
    },

    ListTableNames {
        connection_handle: ConnectionHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<Vec<String>, i64>>,
    },

    CloseTable {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Drop a table from the database.
    /// WARNING: This invalidates any table cache entries.
    DropTable {
        name: String,
        connection_handle: ConnectionHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Drop a database from the connection.
    DropDatabase {
        connection_handle: ConnectionHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Simple "nearest" query.
    QueryNearest {
        limit: u64,
        vector: Vec<f32>,
        table_handle: TableHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<BlobHandle, i64>>,
    },

    /// Request to remove a binary blob from memory.
    FreeBlob {
        handle: BlobHandle,
        reply_sender: tokio::sync::oneshot::Sender<Result<(), i64>>,
    },

    /// Query the length of a blob
    BlobLen {
        handle: BlobHandle,
        reply_sender: tokio::sync::oneshot::Sender<Option<isize>>,
    },

    /// Get Blob Pointer
    GetBlobPointer {
        handle: BlobHandle,
        reply_sender: tokio::sync::oneshot::Sender<Option<Arc<Vec<u8>>>>,
    },

    /// Gracefully shut down the event-loop.
    Quit,
}