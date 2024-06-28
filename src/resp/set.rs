use super::{calc_total_length, parse_length, CAPACITY, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError, RespFrame};
use bytes::{Buf, BytesMut};
use derive_more::{Deref, From};
use std::{collections::HashSet, hash::Hash};

#[derive(Debug, Clone, Deref, PartialEq, Eq, From)]
pub struct RespSet(pub(crate) HashSet<RespFrame>);

// Set "~<number-of-elements>\r\n<element-1>...<element-n>" decode to RespSet
impl RespDecoder for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let mut set = HashSet::with_capacity(len);
        if len == 0 {
            return Ok(RespSet::new(set));
        }
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            set.insert(frame);
        }
        Ok(RespSet::new(set))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// Set format "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncoder for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend(frame.encode());
        }
        buf
    }
}

impl RespSet {
    pub fn new(set: impl Into<HashSet<RespFrame>>) -> Self {
        RespSet(set.into())
    }
}

impl Hash for RespSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.iter().for_each(|frame| frame.hash(state));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespDouble;

    #[test]
    fn test_set_encode() {
        let mut hash_set = HashSet::new();
        hash_set.insert(RespDouble::new(2024.0925).into());
        hash_set.insert(RespDouble::new(2024.0925).into());
        let set: RespFrame = RespSet::new(hash_set).into();
        assert_eq!(set.encode(), b"~1\r\n,2024.0925\r\n");
    }
}
