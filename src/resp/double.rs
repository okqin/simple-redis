use super::{extract_simple_resp, CRLF_LEN};
use crate::{RespDecoder, RespEncoder, RespError};
use bytes::BytesMut;
use derive_more::{Deref, Display, From};
use ordered_float::OrderedFloat;

#[derive(Debug, Clone, Deref, Display, PartialEq, Eq, Hash, From)]
pub struct RespDouble(pub(crate) OrderedFloat<f64>);

// Double ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n" decode to RespDouble
impl RespDecoder for RespDouble {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        let num = s.parse()?;
        Ok(RespDouble::new(num))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Double format ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespEncoder for RespDouble {
    fn encode(self) -> Vec<u8> {
        if self.is_nan() {
            return b",nan\r\n".to_vec();
        }
        if self.is_infinite() {
            return if self.is_sign_negative() {
                b",-inf\r\n".to_vec()
            } else {
                b",inf\r\n".to_vec()
            };
        }
        format!(",{}\r\n", self).into_bytes()
    }
}

impl RespDouble {
    pub fn new(f: f64) -> Self {
        RespDouble(OrderedFloat(f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_double_encode() {
        let d = RespDouble::new(1.23);
        assert_eq!(d.encode(), b",1.23\r\n");

        let d = RespDouble::new(-1.23);
        assert_eq!(d.encode(), b",-1.23\r\n");
    }

    #[test]
    fn test_double_decode() -> Result<()> {
        let mut buf = BytesMut::from(",1.23e2\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(123.0));
        Ok(())
    }

    #[test]
    fn test_double_decode_with_sign() -> Result<()> {
        let mut buf = BytesMut::from(",-1.23e2\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(-123.0));
        Ok(())
    }

    #[test]
    fn test_double_decode_with_exponent_sign() -> Result<()> {
        let mut buf = BytesMut::from(",1.23e-2\r\n");
        let frame = RespDouble::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(0.0123));
        Ok(())
    }
}
