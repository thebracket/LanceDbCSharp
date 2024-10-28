use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

#[derive(Debug, Copy, Clone)]
pub(crate) struct BlobHandle(pub(crate) i64); // Unique record batch id

pub struct BlobHandler {
    next_id: AtomicI64,
    blobs: HashMap<i64, Arc<Vec<u8>>> // Note that we're storing an ARC - reference counted. This is to allow safe transition between threads, and ensure cleanup only happens after there are no more references. But because the map holds a reference, cleanup will never happen until we ask for it.
}

impl BlobHandler {
    pub fn new() -> Self {
        Self {
            next_id: AtomicI64::new(1),
            blobs: HashMap::new()
        }
    }

    pub fn add_blob(&mut self, blob: Vec<u8>) -> BlobHandle {
        let handle = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.blobs.insert(handle, Arc::new(blob));
        BlobHandle(handle)
    }

    pub fn free_if_exists(&mut self, handle: BlobHandle) {
        self.blobs.remove(&handle.0);
    }

    pub fn blob_len(&self, handle: BlobHandle) -> Option<isize> {
        self.blobs.get(&handle.0).map(|b| b.len() as isize)
    }

    pub fn get_blob_arc(&self, handle: BlobHandle) -> Option<Arc<Vec<u8>>> {
        self.blobs.get(&handle.0).cloned()
    }
}