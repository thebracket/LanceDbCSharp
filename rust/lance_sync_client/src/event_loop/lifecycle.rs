use std::sync::atomic::AtomicBool;
use crate::event_loop::event_loop;
use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::errors::clear_errors;
use crate::event_loop::helpers::send_command;

/// This enum represents the possible errors that can occur during
/// the setup of the LanceDB event-loop.
#[repr(i32)]
enum LanceSetupErrors {
    Ok = 0,
    ThreadSpawnError = -1,
}

static ALREADY_SETUP: AtomicBool = AtomicBool::new(false);

pub fn is_already_setup() -> bool {
    ALREADY_SETUP.load(std::sync::atomic::Ordering::Relaxed)
}

/// Spawns a new thread and starts an event-loop ready
/// to work with LanceDB. This function **must** be called before other
/// functions in this library are called.
///
/// Return values: 0 for success, -1 if an error occurred.
#[no_mangle]
pub extern "C" fn setup() -> i32 {
    if ALREADY_SETUP.load(std::sync::atomic::Ordering::Relaxed) {
        eprintln!("Event loop already set up.");
        return LanceSetupErrors::Ok as i32;
    }
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
            ALREADY_SETUP.store(true, std::sync::atomic::Ordering::Relaxed);
            let awaiter = ready_rx.blocking_recv();
            if awaiter.is_err() {
                eprintln!("Error waiting for event loop to start.");
                LanceSetupErrors::ThreadSpawnError as i32
            } else {
                LanceSetupErrors::Ok as i32
            }
        },
        Err(e) => {
            eprintln!("Error spawning thread: {:?}", e);
            LanceSetupErrors::ThreadSpawnError as i32
        }
    }
}

/// Shuts down the event-loop. This function should be called
/// before the program exits (or the library is unloaded).
/// In practice, regular tear-down will stop the event-loop
/// anyway - but this avoids any leakage.
#[no_mangle]
pub extern "C" fn shutdown() -> i32 {
    if !ALREADY_SETUP.load(std::sync::atomic::Ordering::Relaxed) {
        eprintln!("Event loop not set up.");
        return -1;
    }
    clear_errors();
    match send_command(LanceDbCommand::Quit) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}