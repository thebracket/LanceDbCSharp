use crate::event_loop::event_loop;
use anyhow::Result;
use std::sync::atomic::AtomicI64;
use std::sync::Mutex;
use tokio::runtime::Handle;

pub(crate) static INSTANCE_COUNT: AtomicI64 = AtomicI64::new(0);

pub fn is_already_setup() -> bool {
    INSTANCE_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
}

pub(crate) static TOKIO_HANDLE: Mutex<Option<Handle>> = Mutex::new(None);

pub(crate) fn setup() -> Result<()> {
    if is_already_setup() {
        eprintln!("Event loop already set up.");
        return Ok(());
    }
    INSTANCE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let result = std::thread::Builder::new()
        .name("lance_sync_client".to_string())
        .spawn(|| {
            match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => {
                    runtime.block_on(async move { event_loop(ready_tx).await });
                    println!("Event loop finished.");
                    *TOKIO_HANDLE.lock().unwrap() = None;
                }
                Err(e) => {
                    eprintln!("Error creating runtime: {:?}", e);
                }
            }
        });

    match result {
        Ok(_) => {
            let awaiter = ready_rx.blocking_recv();
            match awaiter {
                Ok(handle) => {
                    TOKIO_HANDLE.lock().unwrap().replace(handle);
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Error waiting for event loop to start: {:?}", e);
                    Err(anyhow::anyhow!("Error waiting for event loop to start."))
                }
            }
        }
        Err(e) => {
            eprintln!("Error spawning thread: {:?}", e);
            Err(anyhow::anyhow!("Error spawning thread."))
        }
    }
}
