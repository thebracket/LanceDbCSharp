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
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions};

const BATCH_LENGTH: u32 = 2;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to the database
    let uri = "/tmp/test-batching";
    let db = connect(uri).execute().await?;

    // Create some initial data
    let initial_data = create_some_records()?;
    let tbl = db
        .create_table("my_table", initial_data)
        .execute()
        .await?;

    // Query the data
    let mut options = QueryExecutionOptions::default();
    options.max_batch_length = BATCH_LENGTH;

    let plan = tbl
        .query()
        .limit(8192)
        .nearest_to(&[0.0; 128])?
        .explain_plan(true)
        .await?;
    println!("{:?}", plan);

    let mut stream = tbl
         .query()
         .nearest_to(&[0.0; 128])?
         .limit(8192)
         .execute()
         .await
         .expect("Query creation failed");

    let mut count = 0;
    while let Some(batch) = stream.try_next().await? {
        println!("Received Batch {count}, containing {}", batch.num_rows());

        let n_slices = batch.num_rows() / BATCH_LENGTH as usize;
        for i in 0..n_slices {
            let slice = batch.slice(i * BATCH_LENGTH as usize, BATCH_LENGTH as usize);
            println!("Slice {i}: {}", slice.num_rows());
        }

        count += 1;
        //println!("{batch:?}");
    }

    // Clean up
    db.drop_db().await?;

    // Return success if nothing goes wrong
    Ok(())
}

// Taken from: https://github.com/lancedb/lancedb/issues/1153
fn create_some_records() -> Result<impl IntoArrow> {
    const TOTAL: usize = 65536;
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