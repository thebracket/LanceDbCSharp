use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use anyhow::Result;
use lancedb::{connect, Connection};

pub(crate) struct ConnectionHandle(pub(crate) i64); // Unique identifier for the connection

pub(crate) struct ConnectionFactory {
    next_handle: AtomicI64,
    connections: HashMap<i64, Connection>,
}

impl ConnectionFactory {
    pub(crate) fn new() -> Self {
        Self{
            next_handle: AtomicI64::new(0),
            connections: HashMap::new(),
        }
    }

    pub(crate) async fn create_connection(&mut self, uri: &str) -> Result<ConnectionHandle> {
        // Connect to the database
        let connection = connect(uri).execute().await?;

        // Obtain a new connection to LanceDB
        let new_handle_id = self.next_handle.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Store it in the active connections map
        self.connections.insert(new_handle_id, connection);

        // Return the handle to the caller
        Ok(ConnectionHandle(new_handle_id))
    }

    pub(crate) fn disconnect(&mut self, handle: ConnectionHandle) -> Result<()> {
        // Disconnect from the database
        if !self.connections.contains_key(&handle.0) {
            return Err(anyhow::anyhow!("Connection handle not found"));
        }
        self.connections.remove(&handle.0);
        Ok(())
    }

    pub(crate) fn get_connection(&self, connection_handle: ConnectionHandle) -> Option<&Connection> {
        self.connections.get(&connection_handle.0)
    }
}