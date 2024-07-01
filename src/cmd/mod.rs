mod error;
mod hmap;
mod map;
mod set;

use self::{
    error::CommandError,
    hmap::{HDel, HGet, HGetAll, HKeys, HSet, Hmget, Hmset},
    map::{Del, Echo, Get, Set},
    set::{Sadd, Sismember, Smembers, Srem},
};
use crate::{Backend, RespArray, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Set(Set),
    Get(Get),
    Del(Del),
    HSet(HSet),
    Hmset(Hmset),
    HGet(HGet),
    Hmget(Hmget),
    HDel(HDel),
    HGetAll(HGetAll),
    HKeys(HKeys),
    Echo(Echo),
    Sadd(Sadd),
    Sismember(Sismember),
    Smembers(Smembers),
    Srem(Srem),
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
            Some(RespFrame::BulkString(ref cmd)) => match cmd.to_ascii_lowercase().as_slice() {
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"del" => Ok(Del::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hmget" => Ok(Hmget::try_from(v)?.into()),
                b"hmset" => Ok(Hmset::try_from(v)?.into()),
                b"hdel" => Ok(HDel::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                b"hkeys" => Ok(HKeys::try_from(v)?.into()),
                b"echo" => Ok(Echo::try_from(v)?.into()),
                b"sadd" => Ok(Sadd::try_from(v)?.into()),
                b"sismember" => Ok(Sismember::try_from(v)?.into()),
                b"smembers" => Ok(Smembers::try_from(v)?.into()),
                b"srem" => Ok(Srem::try_from(v)?.into()),
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

impl TryFrom<RespArray> for String {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a one argument".to_string(),
            ));
        }
        match value.first() {
            Some(RespFrame::BulkString(s)) => Ok(String::from_utf8(s.0.clone())?),
            _ => Err(CommandError::InvalidCommandArguments(
                "Argument must be of the BulkString type".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Vec<String> {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() < 1 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a one argument".to_string(),
            ));
        }
        value
            .0
            .into_iter()
            .map(|v| match v {
                RespFrame::BulkString(s) => Ok(String::from_utf8(s.0)?),
                _ => Err(CommandError::InvalidCommandArguments(
                    "Argument must be of the BulkString type".to_string(),
                )),
            })
            .collect::<Result<Vec<String>, CommandError>>()
    }
}

#[derive(Debug)]
pub struct KeyValue {
    key: String,
    value: RespFrame,
}

impl TryFrom<RespArray> for KeyValue {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() != 2 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a two arguments".to_string(),
            ));
        }
        let mut args = value.0.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(KeyValue {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct KeyValues {
    key: String,
    values: Vec<RespFrame>,
}

impl TryFrom<RespArray> for KeyValues {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() < 2 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a two arguments".to_string(),
            ));
        }
        let mut args = value.0.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(KeyValues {
                key: String::from_utf8(key.0)?,
                values: args.collect(),
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct KeyField {
    key: String,
    field: String,
}

impl TryFrom<RespArray> for KeyField {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() != 2 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a two arguments".to_string(),
            ));
        }
        let mut args = value.0.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => {
                Ok(KeyField {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                })
            }
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct KeyFields {
    key: String,
    fields: Vec<String>,
}

impl TryFrom<RespArray> for KeyFields {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() < 2 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a two arguments".to_string(),
            ));
        }
        let mut args = value.0.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(KeyFields {
                key: String::from_utf8(key.0)?,
                fields: args
                    .map(|v| match v {
                        RespFrame::BulkString(s) => Ok(String::from_utf8(s.0)?),
                        _ => Err(CommandError::InvalidCommandArguments(
                            "Argument must be of the BulkString type".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<String>, CommandError>>()?,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Hmap {
    key: String,
    map: Vec<(String, RespFrame)>,
}

impl TryFrom<RespArray> for Hmap {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() < 3 {
            return Err(CommandError::InvalidCommandArguments(
                "Command must have a three arguments".to_string(),
            ));
        }
        // Exclude the number of commands and key parameters.
        if (value.len() - 1) % 2 != 0 {
            return Err(CommandError::InvalidCommandArguments(
                "command must have an even number of arguments".to_string(),
            ));
        }
        let mut args = value.0.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let mut map = Vec::new();
                while let Some(field) = args.next() {
                    match args.next() {
                        Some(value) => match field {
                            RespFrame::BulkString(field) => {
                                map.push((String::from_utf8(field.0)?, value))
                            }
                            _ => {
                                return Err(CommandError::InvalidCommandArguments(
                                    "Invalid key or value".to_string(),
                                ))
                            }
                        },
                        None => {
                            return Err(CommandError::InvalidCommandArguments(
                                "Invalid key or value".to_string(),
                            ))
                        }
                    }
                }
                Ok(Hmap {
                    key: String::from_utf8(key.0)?,
                    map,
                })
            }
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
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

fn extract_args(value: RespArray, start: usize) -> Result<RespArray, CommandError> {
    Ok(value
        .0
        .into_iter()
        .skip(start)
        .collect::<Vec<RespFrame>>()
        .into())
}
