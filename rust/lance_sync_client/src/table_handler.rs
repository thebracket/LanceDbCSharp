use std::collections::HashMap;
use lancedb::{Connection, Table};
use lancedb::arrow::IntoArrow;
use anyhow::Result;

pub struct TableHandler {
    tables: HashMap<String, Table>,
}

impl TableHandler {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub async fn add_table(&mut self, table_name: &str, db: &Connection, data: impl IntoArrow) -> Result<()> {
        let table = db.create_table(table_name, data)
            .execute()
            .await
            .inspect_err(|e| eprintln!("Error creating table: {e:?}"))
            ?;
        self.tables.insert(table_name.to_string(), table);
        Ok(())
    }
}