use super::{calc_total_length, check_resp2_null, parse_length, CAPACITY, CRLF_LEN, RESP2_NULL};
use crate::{RespDecoder, RespEncoder, RespError, RespFrame};
use bytes::{Buf, BytesMut};
use derive_more::{Deref, From};

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

// Arrays "*<number-of-elements>\r\n<element-1>...<element-n>" decode to RespArray
impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            buf.advance(Self::PREFIX.len() + RESP2_NULL.len());
            return Ok(RespArray::new(vec![]));
        }

        let (end, arr_len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, arr_len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = Vec::with_capacity(arr_len);

        if arr_len == 0 {
            return Ok(RespArray::new(frames));
        }

        for _ in 0..arr_len {
            frames.push(RespFrame::decode(buf)?);
        }

        Ok(RespArray::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// Arrays format "*<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncoder for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("*{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend(frame.encode());
        }
        buf
    }
}

impl RespArray {
    pub fn new(frames: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(frames.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, SimpleString};
    use anyhow::Result;

    #[test]
    fn test_array_encode() {
        let array: RespFrame = RespArray::new(vec![
            SimpleString::new("foo").into(),
            SimpleString::new("bar").into(),
            BulkString::new("foobar").into(),
            RespArray::new(vec![64.into()]).into(),
        ])
        .into();
        assert_eq!(
            array.encode(),
            b"*4\r\n+foo\r\n+bar\r\n$6\r\nfoobar\r\n*1\r\n:64\r\n"
        );
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n+simple\r\n:100\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespArray::new(vec![SimpleString::new("simple").into(), 100i64.into()]).into()
        );

        let mut buf = BytesMut::from("*0\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new(vec![]).into());

        let mut buf = BytesMut::from("*3\r\n+simple\r\n:100\r\nset\r\n");
        let frame = RespFrame::decode(&mut buf);
        assert_eq!(
            frame,
            Err(RespError::InvalidFrame(
                "data: [115, 101, 116, 13, 10]".to_string()
            ))
        );

        let mut buf = BytesMut::from("*3\r\n$6\r\nfoobar\r\n:100\r\n$3\r\nset\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespArray::new(vec![
                BulkString::new(b"foobar".to_vec()).into(),
                100i64.into(),
                BulkString::new(b"set".to_vec()).into()
            ])
        );
        Ok(())
    }

    #[test]
    fn test_array_decode_resp2_null() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let arr = RespArray::decode(&mut buf)?;
        assert_eq!(arr, RespArray::new(vec![]));
        Ok(())
    }
}
