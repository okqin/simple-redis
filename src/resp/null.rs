use super::{extract_simple_resp, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::{Buf, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RespNull;

// Null "_\r\n" decode to RespNull
impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        buf.advance(end + CRLF_LEN);
        Ok(RespNull)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(3)
    }
}

// Null format "_\r\n"
impl RespEncoder for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_null() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let null = RespNull::decode(&mut buf)?;
        assert_eq!(null, RespNull);

        let buf = RespNull.encode();
        assert_eq!(buf, b"_\r\n");
        Ok(())
    }
}
