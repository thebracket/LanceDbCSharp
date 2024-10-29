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
mod connection_handler;
mod testing;
mod table_handler;
mod serialization;

