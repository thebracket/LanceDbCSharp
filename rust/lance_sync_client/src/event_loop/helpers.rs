use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::{setup, COMMAND_SENDER};
use anyhow::Result;
use crate::event_loop::lifecycle::INSTANCE_COUNT;

pub(super) fn send_command(command: LanceDbCommand) -> Result<()> {
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
        tx.blocking_send(command)
            .inspect_err(|e| println!("Error sending command: {:?}", e))?;
        Ok(())
    } else {
        println!("No command sender found.");
        Err(anyhow::anyhow!("No command sender found."))
    }
}