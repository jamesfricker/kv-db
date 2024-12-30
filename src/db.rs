use crate::kv::KvPair;
use crate::skip_list::SkipList;
use crate::wal::Wal;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Key not found")]
    KeyNotFound,
}

pub struct DB<K, V> {
    wal: Wal,
    sl: SkipList<K, V>,
}

impl<K, V> DB<K, V>
where
    K: Clone + Debug + PartialEq + Eq + Serialize + Ord + DeserializeOwned,
    V: Clone + Debug + PartialEq + Eq + Serialize + Ord + DeserializeOwned,
{
    /// Creates a new `DB` with a backing WAL file and an in-memory SkipList.
    /// Replays the WAL so the SkipList reflects on-disk contents.
    pub fn new(location: &str, max_level: usize) -> Self {
        // Initialize the WAL
        let wal = Wal::new(location.to_string()).expect("Wal could not be created properly");

        // Initialize the SkipList
        let mut sl = SkipList::new(max_level);

        // Replay existing WAL contents to restore in-memory data
        let existing = wal.read::<K, V>().unwrap_or_default();
        for KvPair { key, value } in existing {
            // Ignore errors here (e.g. duplicates) or handle them as you like
            let _ = sl.put(key, value);
        }

        DB { wal, sl }
    }

    /// Inserts (or updates) a key-value pair in the DB, writing to WAL first.
    pub fn put(&mut self, key: K, value: V) -> Result<(), DatabaseError> {
        let kv = KvPair {
            key: key.clone(),
            value: value.clone(),
        };

        // Write to WAL
        self.wal
            .append(kv)
            .map_err(|_| DatabaseError::KeyNotFound)?;

        // Put in the SkipList
        self.sl
            .put(key, value)
            .map_err(|_| DatabaseError::KeyNotFound)?;

        Ok(())
    }

    /// Retrieves a reference to the value for the given key if it exists.
    pub fn get(&self, key: &K) -> Result<&V, DatabaseError> {
        self.sl.get(key).map_err(|_| DatabaseError::KeyNotFound)
    }
}
