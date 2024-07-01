use super::{extract_args, validate_command, CommandError, CommandExecutor, KeyValue, RESP_OK};
use crate::{Backend, RespArray, RespFrame, RespNull};
use derive_more::Deref;

#[derive(Debug, Deref)]
pub struct Set(KeyValue);

impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.0.key, self.0.value);
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["set"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Get(String);

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["get"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Del(Vec<String>);

impl CommandExecutor for Del {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for key in self.iter() {
            if backend.del(key) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<RespArray> for Del {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["del"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Echo(String);

impl CommandExecutor for Echo {
    fn execute(self, _backend: &Backend) -> RespFrame {
        RespFrame::BulkString(self.0.into())
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["echo"];
        validate_command(&value, &cmd_names)?;

        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
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
        assert_eq!(get.0, "name");
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
        let key_value = KeyValue {
            key: "name".to_string(),
            value: RespFrame::BulkString("victory".into()),
        };
        let cmd = Set(key_value);
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let cmd = Get("name".to_string());
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString("victory".into()));
    }
}
