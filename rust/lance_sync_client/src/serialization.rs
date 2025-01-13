//! Provides FileReader/FileWriter Arrow IPC format conversion to/from byte arrays.

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use std::io::Cursor;

pub(crate) fn schema_to_bytes(schema: &SchemaRef) -> Vec<u8> {
    let mut buf = vec![];
    {
        let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut buf, schema).unwrap();
        fw.finish().unwrap();
    } // Scope to ensure that fw is dropped
    buf
}

pub(crate) fn bytes_to_schema(bytes: &[u8]) -> anyhow::Result<SchemaRef> {
    let reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    Ok(schema)
}

pub(crate) fn batch_to_bytes(batch: &RecordBatch, schema: &SchemaRef) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![];
    {
        let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut buf, schema)?;
        fw.write(batch)?;
        fw.finish()?;
    } // Scope to ensure that fw is dropped
    Ok(buf)
}

pub(crate) fn bytes_to_batch(bytes: &[u8]) -> anyhow::Result<Vec<Result<RecordBatch, ArrowError>>> {
    let reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None)?;
    let batches: Vec<_> = reader.collect::<Vec<_>>().into_iter().collect::<Vec<_>>();
    Ok(batches)
}
