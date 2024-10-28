use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use lancedb::{Connection, Table};
use lancedb::arrow::IntoArrow;
use anyhow::Result;
use arrow_schema::SchemaRef;

#[derive(Debug, Copy, Clone)]
pub(crate) struct TableHandle(pub(crate) i64); // Unique identifier for the connection

pub struct TableHandler {
    next_handle: AtomicI64,
    tables: HashMap<i64, Table>,
}

impl TableHandler {
    pub fn new() -> Self {
        Self {
            next_handle: AtomicI64::new(0),
            tables: HashMap::new(),
        }
    }

    pub async fn add_table(&mut self, table_name: &str, db: &Connection, data: impl IntoArrow) -> Result<TableHandle> {
        let table = db.create_table(table_name, data)
            .execute()
            .await
            .inspect_err(|e| eprintln!("Error creating table: {e:?}"))
            ?;
        let next_handle = self.next_handle.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.tables.insert(next_handle, table);
        Ok(TableHandle(next_handle))
    }

    pub async fn add_empty_table(&mut self, table_name: &str, db: &Connection, schema: SchemaRef) -> Result<TableHandle> {
        let table = db.create_empty_table(table_name, schema.into())
            .execute()
            .await
            .inspect_err(|e| eprintln!("Error creating table: {e:?}"))
            ?;
        let next_handle = self.next_handle.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.tables.insert(next_handle, table);
        Ok(TableHandle(next_handle))
    }

    pub async fn get_table_from_cache(&self, table_handle: TableHandle) -> Result<Table> {
        self.tables.get(&table_handle.0)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Table not found"))
    }

    pub async fn open_table(&mut self, table_name: &str, db: &Connection) -> Result<(TableHandle, SchemaRef)> {
        let table = db.open_table(table_name)
            .execute()
            .await
            .inspect_err(|e| eprintln!("Error opening table: {e:?}"))?;
        let next_handle = self.next_handle.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let schema = table.schema().await?;
        self.tables.insert(next_handle, table);
        Ok((TableHandle(next_handle), schema))
    }

    pub async fn drop_table(&mut self, name: &str, db: &Connection) -> Result<()> {
        // Remove from the table cache
        let handles_to_remove: Vec<i64> = self.tables.iter()
            .filter(|(_, table)| table.name() == name)
            .map(|(handle, _)| *handle)
            .collect();

        for handle in handles_to_remove {
            self.tables.remove(&handle);
        }

        // Perform the drop
        db.drop_table(name)
            .await
            .inspect_err(|e| eprintln!("Error dropping table: {e:?}"))?;
        Ok(())
    }

    pub async fn release_table_handle(&mut self, table_handle: TableHandle) {
        self.tables.remove(&table_handle.0);
    }
}