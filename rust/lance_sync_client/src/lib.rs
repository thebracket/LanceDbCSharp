#![warn(missing_docs)]
#![warn(clippy::unwrap_used)]

//! FFI-bindings for LanceDB. This crate provides a C ABI for LanceDB,
//! suitable for use in other languages. A C# client that uses this
//! library is provided in the `lance_sync_client` crate.
//!
//! The bindings are synchronous at this point. However, LanceDB itself
//! uses Tokio for asynchronous execution - so this API requires a
//! "setup" call to start the Tokio runtime on its own set of threads.
//! Hopefully, we can de-complicate this a bit in the future.

/// The maximum number of commands that can be queued up for processing.
/// This should be moved to a configuration item in the future.
const MAX_COMMANDS: usize = 100;

mod event_loop;
mod connection_handle;
mod testing;
pub(crate) mod batch_handler;
mod table_handler;
mod blob_handler;
mod serialization;

pub use event_loop::connect;
pub use event_loop::submit_record_batch;
pub use event_loop::disconnect;
pub use event_loop::free_record_batch;
pub use event_loop::create_table;
pub use event_loop::query_nearest_to;
pub use event_loop::free_blob;
pub use event_loop::blob_len;
pub use event_loop::get_blob_data;
pub use event_loop::{get_error_message, free_error_message};
