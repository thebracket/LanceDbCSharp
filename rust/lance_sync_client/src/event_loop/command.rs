use crate::connection_handler::ConnectionHandle;
use crate::table_handler::TableHandle;
use arrow_schema::SchemaRef;
use std::ffi::c_char;

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

    /// Drop a database from the connection.
    DropDatabase { connection_handle: ConnectionHandle },

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

    /// Gracefully shut down the event-loop.
    Quit {
        reply_sender: tokio::sync::oneshot::Sender<()>,
    },
}

/// Index types that can be created.
#[derive(Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum IndexType {
    BTree=1,
    Bitmap=2,
    LabelList=3,
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