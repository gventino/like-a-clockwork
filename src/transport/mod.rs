pub mod binary;
pub mod json;
pub mod text;

use std::collections::HashMap;

pub trait HeaderMap {
    fn get(&self, key: &str) -> Option<&str>;
    fn set(&mut self, key: String, value: String);
    fn keys(&self) -> Vec<String>;
}

pub trait BinaryHeaderMap {
    fn get(&self, key: &str) -> Option<&[u8]>;
    fn set(&mut self, key: String, value: Vec<u8>);
    fn keys(&self) -> Vec<String>;
}

impl HeaderMap for HashMap<String, String> {
    fn get(&self, key: &str) -> Option<&str> {
        HashMap::get(self, key).map(|v| v.as_str())
    }

    fn set(&mut self, key: String, value: String) {
        self.insert(key, value);
    }

    fn keys(&self) -> Vec<String> {
        self.keys().cloned().collect()
    }
}

impl BinaryHeaderMap for HashMap<String, Vec<u8>> {
    fn get(&self, key: &str) -> Option<&[u8]> {
        HashMap::get(self, key).map(|v| v.as_slice())
    }

    fn set(&mut self, key: String, value: Vec<u8>) {
        self.insert(key, value);
    }

    fn keys(&self) -> Vec<String> {
        self.keys().cloned().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("malformed value for key '{key}': {reason}")]
    MalformedValue { key: String, reason: String },

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("missing required key: {0}")]
    MissingKey(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashmap_header_map_set_and_get() {
        let mut map = HashMap::<String, String>::new();
        map.set("X-Key".to_string(), "value".to_string());
        assert_eq!(HeaderMap::get(&map, "X-Key"), Some("value"));
    }

    #[test]
    fn hashmap_header_map_get_missing_returns_none() {
        let map = HashMap::<String, String>::new();
        assert_eq!(HeaderMap::get(&map, "missing"), None);
    }

    #[test]
    fn hashmap_header_map_keys() {
        let mut map = HashMap::<String, String>::new();
        map.set("A".to_string(), "1".to_string());
        map.set("B".to_string(), "2".to_string());
        let mut keys = HeaderMap::keys(&map);
        keys.sort();
        assert_eq!(keys, vec!["A", "B"]);
    }

    #[test]
    fn hashmap_binary_header_map_set_and_get() {
        let mut map = HashMap::<String, Vec<u8>>::new();
        map.set("key".to_string(), vec![1, 2, 3]);
        assert_eq!(BinaryHeaderMap::get(&map, "key"), Some([1u8, 2, 3].as_slice()));
    }

    #[test]
    fn hashmap_binary_header_map_get_missing_returns_none() {
        let map = HashMap::<String, Vec<u8>>::new();
        assert_eq!(BinaryHeaderMap::get(&map, "missing"), None);
    }

    #[test]
    fn hashmap_binary_header_map_keys() {
        let mut map = HashMap::<String, Vec<u8>>::new();
        map.set("X".to_string(), vec![]);
        map.set("Y".to_string(), vec![]);
        let mut keys = BinaryHeaderMap::keys(&map);
        keys.sort();
        assert_eq!(keys, vec!["X", "Y"]);
    }
}
