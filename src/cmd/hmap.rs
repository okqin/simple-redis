use super::{
    extract_args, validate_command, CommandError, CommandExecutor, HGet, HGetAll, HSet, RESP_OK,
};
use crate::{resp::RespNull, Backend, RespArray, RespBulkString, RespFrame};

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        let hmap = backend.hmap.get(&self.key);
        match hmap {
            Some(hmap) => {
                let mut data = Vec::with_capacity(hmap.len());
                for v in hmap.iter() {
                    let key = v.key().to_owned();
                    data.push((key, v.value().clone()));
                }
                if self.sort {
                    data.sort_by(|a, b| a.0.cmp(&b.0));
                }
                let ret = data
                    .into_iter()
                    .flat_map(|(k, v)| vec![RespBulkString::from(k).into(), v])
                    .collect::<Vec<RespFrame>>();

                RespArray::new(ret).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
                sort: false,
            }),
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{resp::RespDecoder, RespBulkString};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hset_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"*4\r\n$4\r\nhset\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n",
        );
        let input = RespArray::decode(&mut buf)?;

        let cmd = HSet::try_from(input)?;
        assert_eq!(cmd.key, "myhash");
        assert_eq!(cmd.field, "field");
        assert_eq!(
            cmd.value,
            RespFrame::BulkString(RespBulkString::new("value"))
        );

        Ok(())
    }

    #[test]
    fn test_hget_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n");
        let input = RespArray::decode(&mut buf)?;
        let cmd = HGet::try_from(input)?;
        assert_eq!(cmd.key, "myhash");
        assert_eq!(cmd.field, "field");

        Ok(())
    }

    #[test]
    fn test_hgetall_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$7\r\nhgetall\r\n$6\r\nmyhash\r\n");
        let input = RespArray::decode(&mut buf)?;

        let cmd = HGetAll::try_from(input)?;
        assert_eq!(cmd.key, "myhash");
        Ok(())
    }

    #[test]
    fn test_hgetall_cmd_execute() {
        let backend = Backend::new();
        let cmd = HSet {
            key: "family".to_string(),
            field: "name".to_string(),
            value: RespFrame::BulkString(RespBulkString::new("Vic")),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let cmd = HSet {
            key: "family".to_string(),
            field: "age".to_string(),
            value: RespFrame::Integer(10.into()),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let cmd = HGetAll {
            key: "family".to_string(),
            sort: true,
        };
        let resp = cmd.execute(&backend);
        assert_eq!(
            resp,
            RespArray::new([
                RespBulkString::from("age").into(),
                RespFrame::Integer(10),
                RespBulkString::from("name").into(),
                RespFrame::BulkString("Vic".into()),
            ])
            .into()
        );
    }
}
