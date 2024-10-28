//! Provides FileReader/FileWriter Arrow IPC format conversion to/from byte arrays.

use std::io::Cursor;
use arrow_ipc::reader::FileReader;
use arrow_schema::SchemaRef;
use crate::batch_handler::RecBatch;

pub(crate) fn schema_to_bytes(schema: &SchemaRef) -> Vec<u8> {
    let mut buf = vec![];
    {
        let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut buf, schema).unwrap();
        fw.finish().unwrap();
    } // Scope to ensure that fw is dropped
    buf
}

pub(crate) fn bytes_to_record_batch(bytes: &[u8]) -> anyhow::Result<RecBatch> {
    match FileReader::try_new(Cursor::new(bytes), None) {
        Ok(reader) => {
            let batches = reader.collect::<Vec<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            Ok(batches)
        }
        Err(e) => {
            Err(anyhow::anyhow!("Error reading record batch: {e}"))
        }
    }
}