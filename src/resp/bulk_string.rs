use super::{check_resp2_null, parse_length, CRLF_LEN, RESP2_NULL};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::{Buf, BytesMut};
use derive_more::{AsRef, Deref, From};

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, AsRef, From)]
#[from(String, &'static str, &[u8])]
pub struct BulkString(pub(crate) Vec<u8>);

// Bulk string "$<length>\r\n<data>\r\n" decode to RespBulkString
impl RespDecoder for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            buf.advance(Self::PREFIX.len() + RESP2_NULL.len());
            return Ok(BulkString::new(vec![]));
        }

        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let act_len = buf[end + CRLF_LEN..].len();
        if act_len < len + CRLF_LEN {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString::new(data[..len].to_vec()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            return Ok(Self::PREFIX.len() + RESP2_NULL.len());
        }

        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

// Bulk string format "$<length>\r\n<data>\r\n"
impl RespEncoder for BulkString {
    fn encode(self) -> Vec<u8> {
        let length = self.len();
        let mut buf: Vec<u8> = Vec::with_capacity(length + 10);
        buf.extend(format!("${}\r\n", length).into_bytes());
        buf.extend(self.0);
        buf.extend(b"\r\n");
        buf
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bulk_string_encode() {
        let s = BulkString::new("hello");
        assert_eq!(s.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let s = BulkString::decode(&mut buf)?;
        assert_eq!(s, BulkString::new("hello"));
        Ok(())
    }

    #[test]
    fn test_bulk_string_decode_error_not_crlf() {
        let mut buf = BytesMut::from("$5\r\nhello");
        let res = BulkString::decode(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_bulk_string_decode_error_not_prefix() {
        let mut buf = BytesMut::from("hello\r");
        let res = BulkString::decode(&mut buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_bulk_string_expect_length() -> Result<()> {
        let buf = b"$5\r\nhello\r\n";
        let len = BulkString::expect_length(buf)?;
        assert_eq!(len, buf.len());
        Ok(())
    }
}
