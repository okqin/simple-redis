use crate::{
    BulkString, RespArray, RespDecoder, RespDouble, RespError, RespMap, RespNull, RespSet,
    SimpleError, SimpleString,
};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

#[enum_dispatch(RespEncoder)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RespFrame {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    Null(RespNull),
    Boolean(bool),
    Double(RespDouble),
    Map(RespMap),
    Set(RespSet),
}

impl RespDecoder for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut buf_iter = buf.iter().peekable();
        match buf_iter.peek() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'-') => {
                let frame = SimpleError::decode(buf)?;
                Ok(frame.into())
            }
            Some(b':') => {
                let frame = i64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'$') => {
                let frame = BulkString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'*') => {
                let frame = RespArray::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'_') => {
                let frame = RespNull::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'#') => {
                let frame = bool::decode(buf)?;
                Ok(frame.into())
            }
            Some(b',') => {
                let frame = RespDouble::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'%') => {
                let frame = RespMap::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'~') => {
                let frame = RespSet::decode(buf)?;
                Ok(frame.into())
            }
            None => Err(RespError::FrameNotComplete),
            _ => Err(RespError::InvalidFrame(format!("data: {:?}", buf))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut buf_iter = buf.iter().peekable();
        match buf_iter.peek() {
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b',') => RespDouble::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            _ => Err(RespError::InvalidFrame(format!("data: {:?}", buf))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_resp_frame_decode() -> Result<()> {
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::SimpleString(SimpleString::from("OK")));

        let mut buf = BytesMut::from("-ERR unknown command 'foobar'\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::SimpleError(SimpleError::from("ERR unknown command 'foobar'"))
        );

        let mut buf = BytesMut::from(":1000\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Integer(1000));

        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::BulkString(BulkString::from("foobar")));

        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::Array(RespArray::from(vec![
                RespFrame::BulkString(BulkString::from("foo")),
                RespFrame::BulkString(BulkString::from("bar"))
            ]))
        );

        let mut buf = BytesMut::from("_\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Null(RespNull));

        let mut buf = BytesMut::from("#t\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Boolean(true));

        let mut buf = BytesMut::from("#f\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Boolean(false));

        let mut buf = BytesMut::from(",2.24\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Double(RespDouble::new(2.24)));

        let mut buf = BytesMut::from("%1\r\n+foo\r\n+bar\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::Map(RespMap::from(HashMap::from_iter([(
                SimpleString::from("foo").into(),
                SimpleString::from("bar").into()
            )])))
        );

        let mut buf = BytesMut::from("~2\r\n+foo\r\n+bar\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::Set(RespSet::from(HashSet::from_iter(vec![
                SimpleString::from("foo").into(),
                SimpleString::from("bar").into()
            ])))
        );

        Ok(())
    }
}
