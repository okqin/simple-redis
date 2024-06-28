use crate::RespFrame;
use dashmap::DashMap;
use derive_more::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Deref, Default)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug, Default)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,
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

    pub fn hkeys(&self, key: &str) -> Option<Vec<String>> {
        self.hmap
            .get(key)
            .map(|v| v.iter().map(|v| v.key().to_owned()).collect())
    }

    pub fn hdel(&self, key: &str, field: &str) -> bool {
        self.hmap
            .get(key)
            .map(|v| v.remove(field).is_some())
            .unwrap_or(false)
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
