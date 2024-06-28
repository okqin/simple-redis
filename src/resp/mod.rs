mod array;
mod bool;
mod bulk_string;
mod double;
mod frame;
mod integer;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    array::RespArray, bulk_string::BulkString, double::RespDouble, frame::RespFrame, map::RespMap,
    null::RespNull, set::RespSet, simple_error::SimpleError, simple_string::SimpleString,
};

const CAPACITY: usize = 4096;
const RESP2_NULL: &str = "-1\r\n";
const CRLF_LEN: usize = b"\r\n".len();

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecoder: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;

    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Debug, Error, PartialEq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Frame is not complete")]
    FrameNotComplete,

    #[error("Invalid integer: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Invalid float: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}

fn extract_simple_resp(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::FrameNotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrame(format!(
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
