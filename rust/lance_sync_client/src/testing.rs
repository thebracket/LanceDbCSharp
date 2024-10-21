//! Please ignore this file - it's being used as a scratchpad to test some
//! concepts. This file will be deleted once the concepts are validated.

use std::sync::Arc;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_array::types::Float32Type;
use arrow_schema::{DataType, Field, Schema};

// Taken from: https://github.com/lancedb/lancedb/issues/1153

fn get_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                128,
            ),
            true,
        ),
    ]))
}

fn create_some_records() -> anyhow::Result<Vec<RecordBatch>> {
    const TOTAL: usize = 1000;
    const DIM: usize = 128;

    let schema = get_schema();

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
    ).flatten().collect::<Vec<RecordBatch>>();
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Cursor;
    use std::path::Path;
    use arrow_ipc::reader::FileReader;
    use arrow_ipc::writer::FileWriter;
    use super::*;

    /// Test that we can create records, serialize with "arrow_ipc" to a byte
    /// array, and deserialize back to records. This validates that the
    /// serialization and deserialization process works as expected. It may
    /// provide a shortcut to working with the Arrow IPC format via C#.
    #[test]
    fn test_create_some_records() {
        let schema = get_schema();
        let records = create_some_records().unwrap();
        let n_batches = records.len();
        // Create a buffered writer to write to a vector of u8
        let mut buf = vec![];
        let mut fw = FileWriter::try_new(&mut buf, &schema).unwrap();
        for record in records.iter() {
            fw.write(&record).unwrap();
        }
        fw.finish().unwrap();
        std::mem::drop(fw);

        // Now reverse the process. Use a FileReader to get the contents back.
        let reader = FileReader::try_new(Cursor::new(&buf), None).unwrap();
        let batches = reader.collect::<Vec<_>>()
            .into_iter()
            .flatten()
            .collect::<Vec<RecordBatch>>();
        assert_eq!(n_batches, batches.len());
        batches.iter().zip(records.iter()).for_each(|(b1, b2)| {
            assert_eq!(b1.num_rows(), b2.num_rows());
            assert_eq!(b1.num_columns(), b2.num_columns());
            b1.columns().iter().zip(b2.columns()).for_each(|(c1, c2)| {
                assert_eq!(c1.len(), c2.len());
                assert_eq!(c1.data_type(), c2.data_type());
                assert_eq!(c1.null_count(), c2.null_count());
                assert_eq!(c1.offset(), c2.offset());
            });
        });
    }

    #[test]
    fn write_schema_to_ipc_file() {
        let schema = get_schema();
        let path = Path::new("/tmp/schematest_from_rust");
        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn load_schema_from_cs() {
        // Load a schema with a FileReader from /tmp/schematest - generated by C#
        let path = Path::new("/tmp/schematest");
        let file = File::open(path).unwrap();
        let reader = FileReader::try_new(file, None)
            .unwrap();
        let s = reader.schema();
    }
}