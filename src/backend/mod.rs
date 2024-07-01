use crate::RespFrame;
use dashmap::{DashMap, DashSet};
use derive_more::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Deref, Default)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug, Default)]
pub struct BackendInner {
    map: DashMap<String, RespFrame>,
    hmap: DashMap<String, DashMap<String, RespFrame>>,
    set: DashMap<String, DashSet<RespFrame>>,
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn del(&self, key: &str) -> bool {
        self.map.remove(key).is_some()
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|v| v.get(field).map(|v| v.value().clone()))
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let hmap = self.hmap.entry(key).or_default();
        hmap.insert(field, value);
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }

    pub fn hdel(&self, key: &str, field: &str) -> bool {
        self.hmap
            .get(key)
            .map(|v| v.remove(field).is_some())
            .unwrap_or(false)
    }

    pub fn sadd(&self, key: String, member: RespFrame) -> bool {
        let set = self.set.entry(key).or_default();
        set.insert(member)
    }

    pub fn srem(&self, key: &str, member: &RespFrame) -> bool {
        self.set
            .get(key)
            .map(|v| v.remove(member).is_some())
            .unwrap_or(false)
    }

    pub fn sismember(&self, key: &str, member: &RespFrame) -> bool {
        self.set
            .get(key)
            .map(|v| v.contains(member))
            .unwrap_or(false)
    }

    pub fn smembers(&self, key: &str) -> Option<Vec<RespFrame>> {
        self.set
            .get(key)
            .map(|v| v.iter().map(|v| v.clone()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend() {
        let backend = Backend::new();
        backend.hset(
            "key".into(),
            "field".into(),
            RespFrame::SimpleString("value".into()),
        );
        assert!(backend.hdel("key", "field"));
        assert!(!backend.hdel("key", "field"));
        assert!(!backend.hdel("ke", "field"));
    }
}
