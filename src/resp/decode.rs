use std::collections::{HashMap, HashSet};

use super::{
    RespArray, RespBulkString, RespDecoder, RespDouble, RespError, RespFrame, RespMap, RespNull,
    RespSet, RespSimpleError, RespSimpleString,
};

use bytes::{Buf, BytesMut};

const RESP2_NULL: &str = "-1\r\n";

const CRLF_LEN: usize = b"\r\n".len();

impl RespDecoder for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let mut buf_iter = buf.iter().peekable();
        match buf_iter.peek() {
            Some(b'+') => RespSimpleString::decode(buf),
            Some(b'-') => RespSimpleError::decode(buf),
            Some(b':') => i64::decode(buf),
            Some(b'$') => RespBulkString::decode(buf),
            Some(b'*') => RespArray::decode(buf),
            Some(b'_') => RespNull::decode(buf),
            Some(b'#') => bool::decode(buf),
            Some(b',') => RespDouble::decode(buf),
            Some(b'%') => RespMap::decode(buf),
            Some(b'~') => RespSet::decode(buf),
            _ => Err(super::RespError::InvalidFrame(format!("data: {:?}", buf))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut buf_iter = buf.iter().peekable();
        match buf_iter.peek() {
            Some(b'+') => RespSimpleString::expect_length(buf),
            Some(b'-') => RespSimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'$') => RespBulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b',') => RespDouble::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            _ => Err(super::RespError::InvalidFrame(format!("data: {:?}", buf))),
        }
    }
}

// Simple string "+<str>\r\n" decode to RespSimpleString
impl RespDecoder for RespSimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(RespSimpleString::new(s.to_string()).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Simple error "-<str>\r\n" decode to RespSimpleError
impl RespDecoder for RespSimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(RespSimpleError::new(s.to_string()).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// integer ":[<+|->]<value>\r\n" decode to i64
impl RespDecoder for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        let num = s.parse::<i64>()?;
        Ok(num.into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Bulk string "$<length>\r\n<data>\r\n" decode to RespBulkString
impl RespDecoder for RespBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            buf.advance(Self::PREFIX.len() + RESP2_NULL.len());
            return Ok(RespNull.into());
        }

        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let act_len = buf[end + CRLF_LEN..].len();
        match len.cmp(&(act_len - CRLF_LEN)) {
            std::cmp::Ordering::Less => Err(RespError::InvalidFrameLength(format!(
                "expected length: {}, found: {}",
                len,
                act_len - CRLF_LEN
            ))),
            std::cmp::Ordering::Greater => Err(RespError::FrameNotComplete),
            std::cmp::Ordering::Equal => {
                buf.advance(end + CRLF_LEN);
                let data = buf.split_to(len + CRLF_LEN);
                Ok(RespBulkString::new(data[..len].to_vec()).into())
            }
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            return Ok(Self::PREFIX.len() + RESP2_NULL.len());
        }

        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

// Arrays "*<number-of-elements>\r\n<element-1>...<element-n>" decode to RespArray
impl RespDecoder for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        if check_resp2_null(buf, Self::PREFIX) {
            buf.advance(Self::PREFIX.len() + RESP2_NULL.len());
            return Ok(RespNull.into());
        }

        let (end, arr_len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, arr_len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let mut frames = Vec::with_capacity(arr_len);
        if arr_len == 0 {
            return Ok(RespArray::new(frames).into());
        }
        for _ in 0..arr_len {
            let frame = RespFrame::decode(buf)?;
            frames.push(frame);
        }
        Ok(RespArray::new(frames).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// Null "_\r\n" decode to RespNull
impl RespDecoder for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        buf.advance(end + CRLF_LEN);
        Ok(RespNull.into())
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(3)
    }
}

// Boolean "#<t|f>\r\n" decode to bool
impl RespDecoder for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = &data[Self::PREFIX.len()..end];
        match s {
            b"t" => Ok(true.into()),
            b"f" => Ok(false.into()),
            _ => Err(RespError::InvalidFrameType(format!(
                "expected t or f, found: {}",
                String::from_utf8_lossy(s)
            ))),
        }
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

// Double ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n" decode to RespDouble
impl RespDecoder for RespDouble {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        let num = s.parse()?;
        Ok(RespDouble::new(num).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_resp(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Map "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>" decode to RespMap
impl RespDecoder for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let mut map = HashMap::with_capacity(len);
        if len == 0 {
            return Ok(RespMap::new(map).into());
        }
        for _ in 0..len {
            let key = RespFrame::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.insert(key, value);
        }
        Ok(RespMap::new(map).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// Set "~<number-of-elements>\r\n<element-1>...<element-n>" decode to RespSet
impl RespDecoder for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<RespFrame, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::FrameNotComplete);
        }

        buf.advance(end + CRLF_LEN);
        let mut set = HashSet::with_capacity(len);
        if len == 0 {
            return Ok(RespSet::new(set).into());
        }
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            set.insert(frame);
        }
        Ok(RespSet::new(set).into())
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

fn extract_simple_resp(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::FrameNotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expected start with: {}, found: {:?}",
            prefix, buf
        )));
    }
    let end = find_crlf(buf, 1).ok_or(RespError::FrameNotComplete)?;
    Ok(end)
}

// find nth CRLF in the buffer
fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }

    None
}

fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_resp(buf, prefix)?;
    let len = String::from_utf8_lossy(&buf[prefix.len()..end]).parse()?;
    Ok((end, len))
}

// compatible with RESP2 null
fn check_resp2_null(buf: &[u8], prefix: &str) -> bool {
    buf.starts_with(format!("{}{}", prefix, RESP2_NULL).as_bytes())
}

fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        "%" => {
            for _ in 0..len {
                let key_len = RespFrame::expect_length(data)?;
                data = &data[key_len..];

                let value_len = RespFrame::expect_length(data)?;
                data = &data[value_len..];

                total += key_len + value_len;
            }
            Ok(total)
        }
        _ => Ok(len + total),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_decode_simple_string() -> Result<()> {
        let mut buf = BytesMut::from("+simple\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespSimpleString::new("simple").into());
        Ok(())
    }

    #[test]
    fn test_decode_simple_error() -> Result<()> {
        let mut buf = BytesMut::from("-simple error\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespSimpleError::new("simple error").into());
        Ok(())
    }

    #[test]
    fn test_decode_integer() -> Result<()> {
        let mut buf = BytesMut::from(":100\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, 100i64.into());
        Ok(())
    }

    #[test]
    fn test_decode_bulk_string() -> Result<()> {
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespBulkString::new(b"foobar".to_vec()).into());
        Ok(())
    }

    #[test]
    fn test_decode_array() -> Result<()> {
        let mut buf = BytesMut::from("*2\r\n+simple\r\n:100\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespArray::new(vec![RespSimpleString::new("simple").into(), 100i64.into()]).into()
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
        Ok(())
    }

    #[test]
    fn test_decode_null_array() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespNull.into());
        Ok(())
    }

    #[test]
    fn test_decode_double() -> Result<()> {
        let mut buf = BytesMut::from(",1.23e2\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(123.0).into());
        Ok(())
    }

    #[test]
    fn test_decode_double_with_sign() -> Result<()> {
        let mut buf = BytesMut::from(",-1.23e2\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(-123.0).into());
        Ok(())
    }

    #[test]
    fn test_decode_double_with_exponent_sign() -> Result<()> {
        let mut buf = BytesMut::from(",1.23e-2\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespDouble::new(0.0123).into());
        Ok(())
    }
}
