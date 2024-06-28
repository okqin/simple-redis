mod error;
mod hmap;
mod map;

use self::{
    error::CommandError,
    hmap::{HDel, HGet, HGetAll, HKeys, HSet},
    map::{Del, Get, Set},
};
use crate::{Backend, RespArray, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

// set hello world => "*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
// get hello => "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
// hset map hello world => "*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
// hget map hello => "*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n"
// hgetall map => "*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n"

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Set(Set),
    Get(Get),
    Del(Del),
    HSet(HSet),
    HGet(HGet),
    HDel(HDel),
    HGetAll(HGetAll),
    HKeys(HKeys),
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;
    fn try_from(v: RespFrame) -> Result<Self, Self::Error> {
        match v {
            RespFrame::Array(array) => array.try_into(),
            _ => Err(CommandError::InvalidCommand(
                "Command must be an Array".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;
    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        match v.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_slice() {
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"del" => Ok(Del::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hdel" => Ok(HDel::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                b"hkeys" => Ok(HKeys::try_from(v)?.into()),
                _ => Err(CommandError::InvalidCommand(format!(
                    "unknown command '{}'",
                    String::from_utf8_lossy(cmd.as_ref())
                ))),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first argument".to_string(),
            )),
        }
    }
}

fn validate_command(value: &RespArray, names: &[&'static str]) -> Result<(), CommandError> {
    if value.len() < names.len() {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have at least one argument",
            names.join(" ")
        )));
    }

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn validate_args_hmap_pair(value: &RespArray, names: &[&'static str]) -> Result<(), CommandError> {
    if value.len() <= names.len() + 1 {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have at least one argument",
            names.join(" ")
        )));
    }
    // Exclude the number of commands and key parameters.
    let n = value.len() - names.len() - 1;
    if n % 2 != 0 {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have an even number of arguments",
            names.join(" ")
        )));
    }
    Ok(())
}

fn validate_args_hmap(value: &RespArray, names: &[&'static str]) -> Result<(), CommandError> {
    if value.len() <= names.len() + 1 {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have at least one argument",
            names.join(" ")
        )));
    }
    Ok(())
}

fn validate_args(value: &RespArray, names: &[&'static str]) -> Result<(), CommandError> {
    if value.len() <= names.len() {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have at least one argument",
            names.join(" ")
        )));
    }
    Ok(())
}

fn validate_args_fixed(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != names.len() + n_args {
        return Err(CommandError::InvalidCommandArguments(format!(
            "{} command must have {} arguments",
            names.join(" "),
            n_args
        )));
    }
    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}
