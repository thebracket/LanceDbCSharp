use crate::event_loop::command::LanceDbCommand;
use crate::event_loop::COMMAND_SENDER;
use anyhow::{Result, Context};

pub(super) fn send_command(command: LanceDbCommand) -> Result<()> {
    let tx = COMMAND_SENDER.get()
        .context("Command sender not set up - command channel is closed.")?;
    tx.blocking_send(command)
        .inspect_err(|e| eprintln!("Error sending command: {:?}", e))?;
    Ok(())
}