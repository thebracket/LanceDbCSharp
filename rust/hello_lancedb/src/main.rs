//! This is following the guide at:
//! https://lancedb.github.io/lancedb/basic/

use anyhow::Result;
use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use lancedb::arrow::IntoArrow;
use lancedb::connect;
use std::sync::Arc;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to the database
    let uri = "data/sample_db";
    let db = connect(uri).execute().await?;

    // Create some initial data
    let initial_data = create_some_records()?;
    let tbl = db
        .create_table("my_table", initial_data)
        .execute()
        .await?;

    // Query the data
    let query_test = tbl
        .query()
        .limit(2)
        .nearest_to(&[1.0; 128])?
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("{:?}", query_test);

    // Return success if nothing goes wrong
    Ok(())
}

// Taken from: https://github.com/lancedb/lancedb/issues/1153
fn create_some_records() -> Result<impl IntoArrow> {
    const TOTAL: usize = 1000;
    const DIM: usize = 128;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]));
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..TOTAL).map(|_| Some(vec![Some(1.0); DIM])),
                        DIM as i32,
                    ),
                ),
            ],
        )
            ?]
            .into_iter()
            .map(Ok),
        schema.clone(),
    );
    Ok(Box::new(batches))
}