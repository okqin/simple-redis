use super::extract_simple_resp;
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::BytesMut;

// Boolean "#<t|f>\r\n" decode to bool
impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = &data[Self::PREFIX.len()..end];
        match s {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => Err(RespError::InvalidFrame(format!(
                "expected t or f, found: {}",
                String::from_utf8_lossy(s)
            ))),
        }
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

// Boolean format "#<t|f>\r\n"
impl RespEncoder for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bool() -> Result<()> {
        let mut buf = BytesMut::from("#t\r\n");
        let b = bool::decode(&mut buf)?;
        assert!(b);

        let buf = bool::encode(true);
        assert_eq!(buf, b"#t\r\n");

        let mut buf = BytesMut::from("#f\r\n");
        let b = bool::decode(&mut buf)?;
        assert!(!b);

        let buf = bool::encode(false);
        assert_eq!(buf, b"#f\r\n");
        Ok(())
    }
}
