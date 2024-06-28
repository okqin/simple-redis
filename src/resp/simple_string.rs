use super::{extract_simple_resp, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::BytesMut;
use derive_more::{Deref, From};

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
#[from(&'static str)]
pub struct SimpleString(pub(crate) String);

// Simple string "+<str>\r\n" decode to RespSimpleString
impl RespDecoder for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleString::new(s.to_string()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Simple string format "+<str>\r\n"
impl RespEncoder for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_simple_string_encode() {
        let s = SimpleString::new("hello");
        assert_eq!(s.encode(), b"+hello\r\n");
    }

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("+hello\r\n");
        let s = SimpleString::decode(&mut buf)?;
        assert_eq!(s, SimpleString::new("hello"));
        Ok(())
    }

    #[test]
    fn test_simple_string_decode_error_not_crlf() {
        let mut buf = BytesMut::from("+hello");
        let res = SimpleString::decode(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_simple_string_decode_error_not_prefix() {
        let mut buf = BytesMut::from("hello\r");
        let res = SimpleString::decode(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_simple_string_expect_length() -> Result<()> {
        let buf = b"+hello\r\n";
        let len = SimpleString::expect_length(buf)?;
        assert_eq!(len, buf.len());
        Ok(())
    }
}
