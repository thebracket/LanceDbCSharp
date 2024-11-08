use crate::connection_handler::ConnectionHandle;
use crate::table_handler::TableHandle;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use lancedb::table::AddDataMode;
use std::ffi::c_char;
use lancedb::DistanceType;
use crate::event_loop::VectorDataType;

/// Used to synchronize timings - make sure that the function
/// does not return until all async processing is complete.
pub(crate) type CompletionSender = tokio::sync::oneshot::Sender<()>;

/// Helper function to create a completion pair.
pub(crate) fn get_completion_pair() -> (CompletionSender, tokio::sync::oneshot::Receiver<()>) {
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
        schema_callback: Option<extern "C" fn(bytes: *const u8, len: u64)>,
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
        index_type: IndexType,
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
        batch_callback: Option<extern "C" fn(*const u8, u64)>,
        limit: Option<usize>,
        where_clause: Option<String>,
        with_row_id: bool,
        explain_callback: Option<(bool, extern "C" fn (*const c_char))>,
        selected_columns: Option<Vec<String>>,
        full_text_search: Option<String>,
    },

    VectorQuery {
        connection_handle: ConnectionHandle,
        table_handle: TableHandle,
        batch_callback: Option<extern "C" fn(*const u8, u64)>,
        limit: Option<usize>,
        where_clause: Option<String>,
        with_row_id: bool,
        explain_callback: Option<(bool, extern "C" fn (*const c_char))>,
        selected_columns: Option<Vec<String>>,
        vector_data: VectorDataType,
        metric: DistanceType,
        n_probes: usize,
        refine_factor: u32,
    },

    /// Gracefully shut down the event-loop.
    Quit {
        reply_sender: tokio::sync::oneshot::Sender<()>,
    },
}

/// Index types that can be created.
#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum IndexType {
    BTree = 1,
    Bitmap = 2,
    LabelList = 3,
}

impl From<u32> for IndexType {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::BTree,
            2 => Self::Bitmap,
            3 => Self::LabelList,
            _ => panic!("Invalid index type: {}", value),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum WriteMode {
    Append = 1,
    Overwrite = 2,
}

impl From<u32> for WriteMode {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Append,
            2 => Self::Overwrite,
            _ => panic!("Invalid write mode: {}", value),
        }
    }
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
