use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

pub struct BlobHandler {
    next_id: AtomicI64,
    blobs: HashMap<i64, Arc<Vec<u8>>>
}

impl BlobHandler {
    pub fn new() -> Self {
        Self {
            next_id: AtomicI64::new(1),
            blobs: HashMap::new()
        }
    }

    pub fn add_blob(&mut self, blob: Vec<u8>) -> i64 {
        let handle = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.blobs.insert(handle, Arc::new(blob));
        handle
    }

    pub fn free_if_exists(&mut self, handle: i64) {
        println!("Removing blob with handle {}.", handle);
        self.blobs.remove(&handle);
    }

    pub fn blob_len(&self, handle: i64) -> Option<isize> {
        self.blobs.get(&handle).map(|b| b.len() as isize)
    }

    pub fn get_blob_arc(&self, handle: i64) -> Option<Arc<Vec<u8>>> {
        self.blobs.get(&handle).cloned()
    }
}