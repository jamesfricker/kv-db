use crate::kv::KvPair;
use crate::skip_list::SkipList;
use crate::wal::Wal;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Key not found")]
    KeyNotFound,
}

pub struct DB {
    wal: Wal,
    sl: SkipList,
}

impl DB {
    /// Creates a new `DB` with a backing WAL file and an in-memory SkipList.
    /// Replays the WAL so the SkipList reflects on-disk contents.
    pub fn new(location: &str, max_level: usize) -> Self {
        // Initialize the WAL
        let wal = Wal::new(location.to_string()).expect("Wal could not be created properly");

        // Initialize the SkipList
        let mut sl = SkipList::new(max_level);

        // Replay existing WAL contents to restore in-memory data
        let existing = wal.read().unwrap_or_default();
        for KvPair { key, value } in existing {
            // Ignore errors here (e.g. duplicates) or handle them as you like
            let _ = sl.put(key, value);
        }

        DB { wal, sl }
    }

    /// Inserts (or updates) a key-value pair in the DB, writing to WAL first.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DatabaseError> {
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

        // add a check here to see if we need to flush?

        Ok(())
    }

    /// Retrieves a reference to the value for the given key if it exists.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>, DatabaseError> {
        self.sl.get(key).map_err(|_| DatabaseError::KeyNotFound)
    }

    pub fn flush() {}
}
