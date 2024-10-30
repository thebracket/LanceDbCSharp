use std::sync::atomic::AtomicI64;
use crate::event_loop::event_loop;
use anyhow::Result;

pub(crate) static INSTANCE_COUNT: AtomicI64 = AtomicI64::new(0);

pub fn is_already_setup() -> bool {
    INSTANCE_COUNT.load(std::sync::atomic::Ordering::Relaxed) > 0
}

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
                .worker_threads(4)
                .enable_all()
                .build() {
                Ok(runtime) => {
                    runtime.block_on(async move { event_loop(ready_tx).await });
                }
                Err(e) => {
                    eprintln!("Error creating runtime: {:?}", e);
                }
            }
        });

    match result {
        Ok(_) => {
            let awaiter = ready_rx.blocking_recv();
            if awaiter.is_err() {
                eprintln!("Error waiting for event loop to start.");
                Err(anyhow::anyhow!("Error waiting for event loop to start."))
            } else {
                Ok(())
            }
        },
        Err(e) => {
            eprintln!("Error spawning thread: {:?}", e);
            Err(anyhow::anyhow!("Error spawning thread."))
        }
    }
}