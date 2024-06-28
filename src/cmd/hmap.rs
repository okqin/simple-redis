use super::{
    extract_args, validate_args_fixed, validate_args_hmap, validate_args_hmap_pair,
    validate_command, CommandError, CommandExecutor,
};
use crate::{Backend, BulkString, RespArray, RespFrame, RespNull};

#[derive(Debug, Default)]
pub struct HSet {
    key: String,
    values: Vec<(String, RespFrame)>,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HDel {
    key: String,
    field: Vec<String>,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
    sort: bool,
}

#[derive(Debug)]
pub struct HKeys {
    key: String,
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        let len = self.values.len();
        for v in self.values {
            backend.hset(self.key.clone(), v.0, v.1);
        }
        RespFrame::Integer(len as i64)
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

impl CommandExecutor for HDel {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for field in self.field {
            if backend.hdel(&self.key, &field) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
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
                    .flat_map(|(k, v)| vec![BulkString::from(k).into(), v])
                    .collect::<Vec<RespFrame>>();

                RespArray::new(ret).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for HKeys {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hmap.get(&self.key) {
            Some(hmap) => {
                let keys = hmap
                    .iter()
                    .map(|v| v.key().to_owned())
                    .collect::<Vec<String>>();
                RespArray::new(
                    keys.into_iter()
                        .map(|k| BulkString::new(k).into())
                        .collect::<Vec<RespFrame>>(),
                )
                .into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"])?;
        validate_args_hmap_pair(&value, &["hset"])?;
        let mut args = extract_args(value, 1)?.into_iter();
        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => String::from_utf8(key.0)?,
            _ => {
                return Err(CommandError::InvalidCommandArguments(
                    "Invalid key".to_string(),
                ))
            }
        };
        let mut hset = HSet {
            key,
            values: Vec::new(),
        };
        while let (Some(f), Some(v)) = (args.next(), args.next()) {
            match (f, v) {
                (RespFrame::BulkString(field), value) => {
                    hset.values.push((String::from_utf8(field.0)?, value))
                }
                (_, _) => {
                    return Err(CommandError::InvalidCommandArguments(
                        "Invalid key, field or value".to_string(),
                    ))
                }
            }
        }
        Ok(hset)
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"])?;
        validate_args_fixed(&value, &["hget"], 2)?;
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

impl TryFrom<RespArray> for HDel {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hdel"])?;
        validate_args_hmap(&value, &["hdel"])?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let field = args
                    .map(|f| match f {
                        RespFrame::BulkString(f) => Ok(String::from_utf8(f.0)?),
                        _ => Err(CommandError::InvalidCommandArguments(
                            "Invalid field".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<String>, CommandError>>()?;
                Ok(HDel {
                    key: String::from_utf8(key.0)?,
                    field,
                })
            }
            _ => Err(CommandError::InvalidCommandArguments(
                "Invalid key".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"])?;
        validate_args_fixed(&value, &["hgetall"], 1)?;

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

impl TryFrom<RespArray> for HKeys {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hkeys"])?;
        validate_args_fixed(&value, &["hkeys"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HKeys {
                key: String::from_utf8(key.0)?,
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
    use crate::{resp::RespDecoder, BulkString};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hset_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"*6\r\n$4\r\nhset\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n$4\r\nname\r\n$3\r\nvic\r\n",
        );
        let input = RespArray::decode(&mut buf)?;

        let cmd = HSet::try_from(input)?;
        assert_eq!(cmd.key, "myhash");
        assert_eq!(cmd.values[0].0, "field");
        assert_eq!(
            cmd.values[0].1,
            RespFrame::BulkString(BulkString::new("value"))
        );
        assert_eq!(cmd.values[1].0, "name");
        assert_eq!(
            cmd.values[1].1,
            RespFrame::BulkString(BulkString::new("vic"))
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
            values: vec![
                (
                    "name".to_string(),
                    RespFrame::BulkString(BulkString::new("Vic")),
                ),
                ("age".to_string(), RespFrame::Integer(10.into())),
            ],
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RespFrame::Integer(2));

        let cmd = HGetAll {
            key: "family".to_string(),
            sort: true,
        };
        let resp = cmd.execute(&backend);
        assert_eq!(
            resp,
            RespArray::new([
                BulkString::from("age").into(),
                RespFrame::Integer(10),
                BulkString::from("name").into(),
                RespFrame::BulkString("Vic".into()),
            ])
            .into()
        );
    }
}
