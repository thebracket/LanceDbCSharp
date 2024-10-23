use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;

pub type RecBatch = Vec<Result<RecordBatch, ArrowError>>;

/// Provides an interface for C# to submit record batches (with schema) to Rust,
/// using the Arrow IPC file format. Streaming format is not supported because it
/// varies wildly between Arrow versions; the file format is more stable.
pub struct BatchHandler {
    next_id: AtomicI64,
    batches: HashMap<i64, RecBatch>
}

impl BatchHandler {
    /// Set up the batch handler, ready to hold data.
    pub(crate) fn new() -> Self {
        Self {
            next_id: AtomicI64::new(1),
            batches: HashMap::new()
        }
    }

    pub(crate) fn add_batch(&mut self, batch: RecBatch) -> i64 {
        let handle = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.batches.insert(handle, batch);
        handle
    }

    pub(crate) fn free_if_exists(&mut self, handle: i64) {
        println!("Removing batch with handle {}.", handle);
        self.batches.remove(&handle);
    }

    pub(crate) fn take_batch(&mut self, handle: i64) -> Option<RecBatch> {
        self.batches.remove(&handle)
    }

    pub(crate) fn get_batch(&self, handle: i64) -> Option<&RecBatch> {
        self.batches.get(&handle)
    }

    pub(crate) fn batch_as_bytes(&self, handle: i64) -> Vec<u8> {
        let batches = self.batches.get(&handle).unwrap();
        let mut buf = vec![];
        {
            let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut buf, &batches[0].as_ref().unwrap().schema()).unwrap();
            for record in batches.iter() {
                fw.write(&record.as_ref().unwrap()).unwrap();
            }
            fw.finish().unwrap();
        } // Scope to ensure that fw is dropped
        buf
    }
}