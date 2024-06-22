mod decode;
mod encode;

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use bytes::BytesMut;
use derive_more::{AsRef, Deref, Display, From};
use enum_dispatch::enum_dispatch;
use ordered_float::OrderedFloat;
use thiserror::Error;

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

#[enum_dispatch(RespEncoder)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RespFrame {
    SimpleString(RespSimpleString),
    SimpleError(RespSimpleError),
    Integer(i64),
    BulkString(RespBulkString),
    Array(RespArray),
    Null(RespNull),
    Boolean(bool),
    Double(RespDouble),
    Map(RespMap),
    Set(RespSet),
}

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
pub struct RespSimpleString(pub(crate) String);

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
pub struct RespSimpleError(pub(crate) String);

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, AsRef, From)]
#[from(String, &'static str, &[u8])]
pub struct RespBulkString(pub(crate) Vec<u8>);

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash, From)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RespNull;

#[derive(Debug, Clone, Deref, Display, PartialEq, Eq, Hash, From)]
pub struct RespDouble(pub(crate) OrderedFloat<f64>);

#[derive(Debug, Clone, Deref, PartialEq, Eq, From)]
pub struct RespMap(pub(crate) HashMap<RespFrame, RespFrame>);

#[derive(Debug, Clone, Deref, PartialEq, Eq, From)]
pub struct RespSet(pub(crate) HashSet<RespFrame>);

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecoder: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;

    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

impl RespSimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        RespSimpleString(s.into())
    }
}

impl RespSimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        RespSimpleError(s.into())
    }
}

impl RespBulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        RespBulkString(s.into())
    }
}

impl RespArray {
    pub fn new(frames: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(frames.into())
    }
}

impl RespDouble {
    pub fn new(f: f64) -> Self {
        RespDouble(OrderedFloat(f))
    }
}

impl RespMap {
    pub fn new(map: impl Into<HashMap<RespFrame, RespFrame>>) -> Self {
        RespMap(map.into())
    }
}

impl RespSet {
    pub fn new(set: impl Into<HashSet<RespFrame>>) -> Self {
        RespSet(set.into())
    }
}

impl Hash for RespSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.iter().for_each(|frame| frame.hash(state));
    }
}

impl Hash for RespMap {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.iter().for_each(|(k, v)| {
            k.hash(state);
            v.hash(state);
        });
    }
}
