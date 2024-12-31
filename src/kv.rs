use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]

pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvPair {
    /// Create a new KvPair
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}
