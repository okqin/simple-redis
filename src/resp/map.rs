use super::{calc_total_length, parse_length, CAPACITY, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError, RespFrame};
use bytes::{Buf, BytesMut};
use derive_more::{Deref, From};
use std::{collections::HashMap, hash::Hash};

#[derive(Debug, Clone, Deref, PartialEq, Eq, From)]
pub struct RespMap(pub(crate) HashMap<RespFrame, RespFrame>);

// Map "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>" decode to RespMap
impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let mut map = HashMap::with_capacity(len);
        if len == 0 {
            return Ok(RespMap::new(map));
        }
        for _ in 0..len {
            let key = RespFrame::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.insert(key, value);
        }
        Ok(RespMap::new(map))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// Map format "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespEncoder for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("%{}\r\n", self.len()).into_bytes());
        for (key, value) in self.0 {
            buf.extend(key.encode());
            buf.extend(value.encode());
        }
        buf
    }
}

impl RespMap {
    pub fn new(map: impl Into<HashMap<RespFrame, RespFrame>>) -> Self {
        RespMap(map.into())
    }
}

impl Hash for RespMap {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.iter().for_each(|(k, v)| {
            k.hash(state);
            v.hash(state);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleString;

    #[test]
    fn test_map_encode() {
        let mut hash_map = HashMap::new();
        hash_map.insert(SimpleString::new("foo").into(), 64.into());
        hash_map.insert(SimpleString::new("foo").into(), 128.into());
        let map: RespFrame = RespMap::new(hash_map).into();
        assert_eq!(map.encode(), b"%1\r\n+foo\r\n:128\r\n");
    }
}
