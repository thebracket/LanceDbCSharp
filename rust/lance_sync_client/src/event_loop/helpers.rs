use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::COMMAND_SENDER;
use anyhow::Result;

pub(super) fn send_command(command: LanceDbCommand) -> Result<()> {
    if let Some(tx) = COMMAND_SENDER.get() {
        tx.blocking_send(command)
            .inspect_err(|e| println!("Error sending command: {:?}", e))?;
        Ok(())
    } else {
        println!("No command sender found.");
        Err(anyhow::anyhow!("No command sender found."))
    }
}