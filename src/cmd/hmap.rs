use derive_more::Deref;

use super::{
    extract_args, validate_command, CommandError, CommandExecutor, Hmap, KeyField, KeyFields,
    RESP_OK,
};
use crate::{Backend, BulkString, RespArray, RespFrame, RespNull};

#[derive(Debug, Deref)]
pub struct HSet(Hmap);

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        let len = self.map.len();
        for v in self.0.map {
            backend.hset(self.0.key.clone(), v.0, v.1);
        }
        RespFrame::Integer(len as i64)
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hset"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Hmset(Hmap);

impl CommandExecutor for Hmset {
    fn execute(self, backend: &Backend) -> RespFrame {
        for v in self.0.map {
            backend.hset(self.0.key.clone(), v.0, v.1);
        }
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Hmset {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hmset"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct HGet(KeyField);

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hget"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Hmget(KeyFields);

impl CommandExecutor for Hmget {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut data = Vec::with_capacity(self.fields.len());
        for field in self.fields.iter() {
            match backend.hget(&self.key, field) {
                Some(value) => data.push(value),
                None => data.push(RespFrame::Null(RespNull)),
            }
        }
        RespArray::new(data).into()
    }
}

impl TryFrom<RespArray> for Hmget {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hmget"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct HDel(KeyFields);

impl CommandExecutor for HDel {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for field in self.fields.iter() {
            if backend.hdel(&self.key, field) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<RespArray> for HDel {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hdel"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
    sort: bool,
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        let hmap = backend.hgetall(&self.key);
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

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hgetall"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self {
            key: args.try_into()?,
            sort: false,
        })
    }
}

#[derive(Debug, Deref)]
pub struct HKeys(String);

impl CommandExecutor for HKeys {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hgetall(&self) {
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

impl TryFrom<RespArray> for HKeys {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["hkeys"];
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
    fn test_hset_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            b"*6\r\n$4\r\nhset\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n$4\r\nname\r\n$3\r\nvic\r\n",
        );
        let input = RespArray::decode(&mut buf)?;

        let cmd = HSet::try_from(input)?;
        assert_eq!(cmd.key, "myhash");
        assert_eq!(cmd.map[0].0, "field");
        assert_eq!(
            cmd.map[0].1,
            RespFrame::BulkString(BulkString::new("value"))
        );
        assert_eq!(cmd.map[1].0, "name");
        assert_eq!(cmd.map[1].1, RespFrame::BulkString(BulkString::new("vic")));

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
        let map = Hmap {
            key: "family".to_string(),
            map: vec![
                (
                    "name".to_string(),
                    RespFrame::BulkString(BulkString::new("Vic")),
                ),
                ("age".to_string(), RespFrame::Integer(10.into())),
            ],
        };
        let cmd = HSet(map);
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
