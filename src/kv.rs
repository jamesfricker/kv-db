use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvPair<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> KvPair<K, V> {
    /// Create a new KvPair
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}
