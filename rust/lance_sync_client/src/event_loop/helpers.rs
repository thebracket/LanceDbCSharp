use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::{setup, CompletionSender, ErrorReportFn, LanceDbCommandSet, COMMAND_SENDER};
use anyhow::Result;
use crate::event_loop::lifecycle::INSTANCE_COUNT;

/// Send a command to the event loop. This is intended to be used by the
/// FFI-exposed API to submit calls for processing inside the tokio runtime.
///
/// If the event loop hasn't been initialized, this will cause it to be created.
///
/// # Arguments
///
/// * `command` - The command to send to the event loop.
pub(crate) fn send_command(command: LanceDbCommand, reply_tx: ErrorReportFn, completion_sender: CompletionSender) -> Result<()> {
    let mut tries = 0;
    while INSTANCE_COUNT.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        setup().inspect_err(|e| println!("Error setting up event loop: {:?}", e))?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        tries += 1;
        if tries > 10 {
            return Err(anyhow::anyhow!("Event loop not started."));
        }
    }

    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(LanceDbCommandSet {
            command,
            reply_tx,
            completion_sender,
        })
            .inspect_err(|e| println!("Error sending command: {:?}", e))?;
        Ok(())
    } else {
        println!("No command sender found.");
        Err(anyhow::anyhow!("No command sender found."))
    }
}

/// Macro to send a command to the event loop and wait for completion.
/// This pattern is repeated in many places in the FFI code, so it's
/// been abstracted into a macro.
#[macro_export]
macro_rules! command_from_ffi {
    ($command: expr, $name: expr, $reply_sender: expr) => {
        let (tx, rx) = crate::event_loop::command::get_completion_pair();
        if crate::event_loop::helpers::send_command($command, $reply_sender, tx).is_err() {
            let err = format!("Error sending command: {}", $name);
            report_result(Err(err), $reply_sender, None);
            return;
        };
        rx.blocking_recv().unwrap();
    };
}