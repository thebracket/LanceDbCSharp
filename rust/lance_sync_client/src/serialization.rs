//! Provides FileReader/FileWriter Arrow IPC format conversion to/from byte arrays.

use arrow_schema::SchemaRef;
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
