use crate::connection_handler::ConnectionHandle;
use crate::event_loop::VectorDataType;
use crate::table_handler::TableHandle;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use lancedb::table::AddDataMode;
use lancedb::DistanceType;
use std::ffi::c_char;
use strum::FromRepr;
use crate::BlobCallback;

/// Used to synchronize timings - make sure that the function
/// does not return until all async processing is complete.
pub(crate) type CompletionSender = tokio::sync::oneshot::Sender<(i64, String)>;

/// Helper function to create a completion pair.
pub(crate) fn get_completion_pair() -> (CompletionSender, tokio::sync::oneshot::Receiver<(i64, String)>) {
    tokio::sync::oneshot::channel()
}

/// Commands that can be sent to the LanceDB event-loop.
#[derive(Debug)]
pub(crate) enum LanceDbCommand {
    /// Request to create a new connection to the database.
    ConnectionRequest { uri: String },

    /// Request to disconnect a connection from the database.
    Disconnect { handle: ConnectionHandle },

    /// Request to create a new table in the database.
    CreateTableWithSchema {
        name: String,
        connection_handle: ConnectionHandle,
        schema: SchemaRef,
    },

    /// Open a table in the database.
    OpenTable {
        name: String,
        connection_handle: ConnectionHandle,
        schema_callback: BlobCallback,
    },

    ListTableNames {
        connection_handle: ConnectionHandle,
        string_callback: Option<extern "C" fn(*const c_char)>,
    },

    CloseTable {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
    },

    /// Drop a table from the database.
    /// WARNING: This invalidates any table cache entries.
    DropTable {
        name: String,
        connection_handle: ConnectionHandle,
        ignore_missing: bool,
    },

    RenameTable {
        connection_handle: ConnectionHandle,
        old_name: String,
        new_name: String,
    },

    // TODO: Add Dictionary
    // TODO: Add Table
    /// Drop a database from the connection.
    DropDatabase { connection_handle: ConnectionHandle },

    AddRecordBatch {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        write_mode: WriteMode,
        bad_vector_handling: BadVectorHandling,
        fill_value: f32,
        batch: Vec<Result<RecordBatch, ArrowError>>,
    },

    MergeInsert {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        columns: Option<Vec<String>>,
        when_not_matched_insert_all: bool,
        where_clause: Option<String>,
        when_not_matched_by_source_delete: Option<String>,
        batch: Vec<Result<RecordBatch, ArrowError>>,
    },

    /// Count the number of rows in a table.
    CountRows {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        filter: Option<String>,
    },

    CreateScalarIndex {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        column_name: String,
        index_type: ScalarIndexType,
        replace: bool,
    },

    CreateFullTextIndex {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        columns: Vec<String>,
        with_position: bool,
        replace: bool,
        tokenizer_name: String,
    },

    CreateIndex {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        column_name: String,
        metric: DistanceType,
        num_partitions: u32,
        num_sub_vectors: u32,
        replace: bool,
    },

    Update {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        updates: Vec<(String, String)>,
        where_clause: Option<String>,
        update_callback: Option<extern "C" fn(u64)>,
    },
    //
    // UpdateSQL {
    //     connection_handle: ConnectionHandle,
    //     table_handle: TableHandle,
    //     updates: Vec<(String, String)>,
    //     where_clause: String,
    // },
    //
    DeleteRows {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        where_clause: Option<String>,
    },

    OptimizeTable {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        compaction_callback: extern "C" fn(u64, u64, u64, u64),
        prune_callback: extern "C" fn(u64, u64),
    },

    Query {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        batch_callback: BlobCallback,
        limit: Option<usize>,
        where_clause: Option<String>,
        with_row_id: bool,
        explain_callback: Option<(bool, extern "C" fn(*const c_char))>,
        selected_columns: Option<Vec<String>>,
        full_text_search: Option<String>,
        batch_size: u32,
    },

    VectorQuery {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
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
    },

    /// List indices for a table.
    ListIndices {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        string_callback: Option<extern "C" fn(*const c_char, u32, *const *const c_char, column_count: u64)>,
    },

    /// Get Index Statistics
    GetIndexStats {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        index_name: String,
        callback: Option<extern "C" fn(u32, u32, u64, u64, u64)>,
    },

    /// Gracefully shut down the event-loop.
    Quit {
        reply_sender: tokio::sync::oneshot::Sender<()>,
    },
}

/// Index types that can be created.
#[derive(Debug, Clone, Copy, FromRepr)]
#[repr(u32)]
pub(crate) enum IndexType {
    BTree = 0,
    Bitmap = 1,
    LabelList = 2,
    Fts = 3,
    HnswPq = 4,
    HnswSq = 5,
    IvfPq = 6,
}

/// Scalar Index types that can be created.
#[derive(Debug, Clone, Copy, FromRepr)]
#[repr(u32)]
pub(crate) enum ScalarIndexType {
    BTree = 0,
    Bitmap = 1,
    LabelList = 2,
}

impl From<lancedb::index::IndexType> for IndexType {
    fn from(value: lancedb::index::IndexType) -> Self {
        match value {
            lancedb::index::IndexType::BTree => Self::BTree,
            lancedb::index::IndexType::Bitmap => Self::Bitmap,
            lancedb::index::IndexType::LabelList => Self::LabelList,
            lancedb::index::IndexType::FTS => Self::Fts,
            lancedb::index::IndexType::IvfPq => Self::IvfPq,
            lancedb::index::IndexType::IvfHnswPq => Self::HnswPq,
            lancedb::index::IndexType::IvfHnswSq => Self::HnswSq,
        }
    }
}

#[derive(Debug, Clone, Copy, FromRepr)]
#[repr(u32)]
pub(crate) enum WriteMode {
    Append = 1,
    Overwrite = 2,
}

impl From<WriteMode> for AddDataMode {
    fn from(value: WriteMode) -> Self {
        match value {
            WriteMode::Append => Self::Append,
            WriteMode::Overwrite => Self::Overwrite,
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum BadVectorHandling {
    Error = 1,
    Drop = 2,
    Fill = 3,
}

impl From<u32> for BadVectorHandling {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Error,
            2 => Self::Drop,
            3 => Self::Fill,
            _ => panic!("Invalid bad vector handling: {}", value),
        }
    }
}
