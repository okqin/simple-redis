use super::{extract_args, validate_command, CommandError, CommandExecutor, KeyValue, KeyValues};
use crate::{Backend, RespArray, RespFrame};
use derive_more::Deref;

#[derive(Debug, Deref)]
pub struct Sadd(KeyValues);

impl CommandExecutor for Sadd {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for v in self.0.values {
            if backend.sadd(self.0.key.clone(), v) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<RespArray> for Sadd {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["sadd"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Srem(KeyValues);

impl CommandExecutor for Srem {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut count = 0;
        for v in self.values.iter() {
            if backend.srem(&self.key, v) {
                count += 1;
            }
        }
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<RespArray> for Srem {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["srem"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Sismember(KeyValue);

impl CommandExecutor for Sismember {
    fn execute(self, backend: &Backend) -> RespFrame {
        let result = backend.sismember(&self.key, &self.value);
        if result {
            RespFrame::Integer(1)
        } else {
            RespFrame::Integer(0)
        }
    }
}

impl TryFrom<RespArray> for Sismember {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["sismember"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[derive(Debug, Deref)]
pub struct Smembers(String);

impl CommandExecutor for Smembers {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.smembers(&self) {
            Some(set) => RespFrame::Array(set.into()),
            None => RespFrame::Array(vec![].into()),
        }
    }
}

impl TryFrom<RespArray> for Smembers {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let cmd_names = ["smembers"];
        validate_command(&value, &cmd_names)?;
        let args = extract_args(value, cmd_names.len())?;
        Ok(Self(args.try_into()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sadd() {
        let backend = Backend::new();
        let sadd = Sadd(KeyValues {
            key: "key".into(),
            values: vec![RespFrame::SimpleString("value".into())],
        });
        let resp = sadd.execute(&backend);
        assert_eq!(resp, RespFrame::Integer(1));
    }

    #[test]
    fn test_srem() {
        let backend = Backend::new();
        let sadd = Sadd(KeyValues {
            key: "key".into(),
            values: vec![RespFrame::SimpleString("value".into())],
        });
        sadd.execute(&backend);
        let srem = Srem(KeyValues {
            key: "key".into(),
            values: vec![RespFrame::SimpleString("value".into())],
        });
        let resp = srem.execute(&backend);
        assert_eq!(resp, RespFrame::Integer(1));
    }

    #[test]
    fn test_sismember() {
        let backend = Backend::new();
        let sadd = Sadd(KeyValues {
            key: "key".into(),
            values: vec![RespFrame::SimpleString("value".into())],
        });
        sadd.execute(&backend);
        let sismember = Sismember(KeyValue {
            key: "key".into(),
            value: RespFrame::SimpleString("value".into()),
        });
        let resp = sismember.execute(&backend);
        assert_eq!(resp, RespFrame::Integer(1));
    }

    #[test]
    fn test_smembers() {
        let backend = Backend::new();
        let sadd = Sadd(KeyValues {
            key: "key".into(),
            values: vec![RespFrame::SimpleString("value".into())],
        });
        sadd.execute(&backend);
        let smembers = Smembers("key".into());
        let resp = smembers.execute(&backend);
        assert_eq!(
            resp,
            RespFrame::Array(vec![RespFrame::SimpleString("value".into())].into())
        );
    }
}
