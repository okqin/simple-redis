mod encode;

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use derive_more::{Deref, Display};
use enum_dispatch::enum_dispatch;
use ordered_float::OrderedFloat;

#[enum_dispatch(RespEncoder)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RespFrame {
    SimpleString(RespSimpleString),
    SimpleError(RespSimpleError),
    Integer(i64),
    BulkString(RespBulkString),
    BulkStringNull(RespBulkStringNull),
    Array(RespArray),
    ArrayNull(RespArrayNull),
    Null(RespNull),
    Boolean(bool),
    Double(RespDouble),
    Map(RespMap),
    Set(RespSet),
}

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash)]
pub struct RespSimpleString(String);

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash)]
pub struct RespSimpleError(String);

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash)]
pub struct RespBulkString(Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RespBulkStringNull;

#[derive(Debug, Clone, Deref, PartialEq, Eq, Hash)]
pub struct RespArray(Vec<RespFrame>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RespArrayNull;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RespNull;

#[derive(Debug, Clone, Deref, Display, PartialEq, Eq, Hash)]
pub struct RespDouble(OrderedFloat<f64>);

#[derive(Debug, Clone, Deref, PartialEq, Eq)]
pub struct RespMap(HashMap<RespFrame, RespFrame>);

#[derive(Debug, Clone, Deref, PartialEq, Eq)]
pub struct RespSet(HashSet<RespFrame>);

#[enum_dispatch]
pub trait RespEncoder {
    fn encode(self) -> Vec<u8>;
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
