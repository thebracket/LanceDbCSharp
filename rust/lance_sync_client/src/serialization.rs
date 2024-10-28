//! Provides FileReader/FileWriter Arrow IPC format conversion to/from byte arrays.

use arrow_schema::SchemaRef;

pub(crate) fn schema_to_bytes(schema: &SchemaRef) -> Vec<u8> {
    let mut buf = vec![];
    {
        let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut buf, schema).unwrap();
        fw.finish().unwrap();
    } // Scope to ensure that fw is dropped
    buf
}