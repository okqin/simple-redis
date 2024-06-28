use crate::{RespError, RespFrame};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid command arguments: {0}")]
    InvalidCommandArguments(String),
    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("Invalid UTF-8: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

impl From<CommandError> for RespFrame {
    fn from(err: CommandError) -> Self {
        match err {
            CommandError::InvalidCommand(msg) => RespFrame::SimpleError(msg.into()),
            CommandError::InvalidCommandArguments(_) => {
                RespFrame::SimpleError("ERR wrong number of arguments for command".into())
            }
            _ => RespFrame::SimpleError("ERR internal error".into()),
        }
    }
}
