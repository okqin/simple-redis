use super::{
    extract_args, validate_args, validate_args_fixed, validate_command, CommandError,
    CommandExecutor, RESP_OK,
};
use crate::{Backend, RespArray, RespFrame, RespNull};

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Del {
    key: Vec<String>,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone()
    }
}

impl CommandExecutor for Del {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for key in self.key {
            if backend.del(&key) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"])?;
        validate_args_fixed(&value, &["set"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"])?;
        validate_args_fixed(&value, &["get"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Del {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["del"])?;
        validate_args(&value, &["del"])?;

        let keys = extract_args(value, 1)?
            .into_iter()
            .map(|k| match k {
                RespFrame::BulkString(key) => Ok(String::from_utf8(key.0)?),
                _ => Err(CommandError::InvalidCommandArguments(
                    "Invalid key".to_string(),
                )),
            })
            .collect::<Result<Vec<String>, CommandError>>()?;

        Ok(Del { key: keys })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{resp::RespDecoder, BulkString};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_get_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$4\r\nname\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let get = Get::try_from(frame)?;
        assert_eq!(get.key, "name");
        Ok(())
    }

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$4\r\nname\r\n$7\r\nvictory\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let set = Set::try_from(frame)?;
        assert_eq!(set.key, "name");
        assert_eq!(set.value, RespFrame::BulkString(BulkString::new("victory")));
        Ok(())
    }

    #[test]
    fn test_set_and_get_cmd_execute() {
        let backend = Backend::new();
        let cmd = Set {
            key: "name".to_string(),
            value: RespFrame::BulkString("victory".into()),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let cmd = Get {
            key: "name".to_string(),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString("victory".into()));
    }
}
