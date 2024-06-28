use super::{extract_simple_resp, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::BytesMut;
use derive_more::{Deref, From};

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
#[from(&str, String)]
pub struct SimpleError(pub(crate) String);

// Simple error "-<str>\r\n" decode to RespSimpleError
impl RespDecoder for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleError::new(s.to_string()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Simple error format "-<str>\r\n"
impl RespEncoder for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespError;
    use anyhow::Result;

    #[test]
    fn test_simple_error() -> Result<()> {
        let s = "-ERR unknown command 'foobar'\r\n";
        let mut buf = BytesMut::from(s);
        let resp = SimpleError::decode(&mut buf)?;
        assert_eq!(resp, SimpleError::new("ERR unknown command 'foobar'"));

        let buf = resp.encode();
        assert_eq!(buf, s.as_bytes());

        let s = "-ERR unknown command 'foobar'\r\n";
        let buf = s.as_bytes();
        let len = SimpleError::expect_length(buf).unwrap();
        assert_eq!(len, s.len());
        Ok(())
    }

    #[test]
    fn test_simple_error_empty() -> Result<()> {
        let s = "-\r\n";
        let mut buf = BytesMut::from(s);
        let resp = SimpleError::decode(&mut buf)?;
        assert_eq!(resp, SimpleError::new(""));

        let buf = resp.encode();
        assert_eq!(buf, s.as_bytes());

        let s = "-\r\n";
        let buf = s.as_bytes();
        let len = SimpleError::expect_length(buf).unwrap();
        assert_eq!(len, s.len());
        Ok(())
    }

    #[test]
    fn test_simple_error_incomplete() {
        let s = "-ERR unknown command 'foobar'\r\n";
        let buf = s.as_bytes();
        let mut buf = BytesMut::from(&buf[..buf.len() - 1]);
        let resp = SimpleError::decode(&mut buf);
        assert_eq!(resp, Err(RespError::FrameNotComplete));
    }
}
