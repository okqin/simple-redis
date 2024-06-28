use super::{extract_simple_resp, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::BytesMut;

// integer ":[<+|->]<value>\r\n" decode to i64
impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        let num = s.parse::<i64>()?;
        Ok(num)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// integer format ":[<+|->]<value>\r\n"
impl RespEncoder for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{}\r\n", self).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_integer() -> Result<()> {
        let mut buf = BytesMut::from(":123\r\n");
        let num = i64::decode(&mut buf)?;
        assert_eq!(num, 123);

        let buf = i64::encode(123);
        assert_eq!(buf, b":123\r\n");

        let mut buf = BytesMut::from(":-123\r\n");
        let num = i64::decode(&mut buf)?;
        assert_eq!(num, -123);

        let buf = i64::encode(-123);
        assert_eq!(buf, b":-123\r\n");
        Ok(())
    }
}
