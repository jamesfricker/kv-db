use log::debug;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::cmp::Ordering;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SkipListError {
    #[error("Key not found")]
    KeyNotFound,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    pub forward: Vec<Option<usize>>,
}

pub struct SkipList {
    pub head: usize, // Always points to the sentinel node
    pub nodes: Vec<Node>,
    max_level: usize,
    pub current_level: usize,

    // Reusable vector to avoid re-allocating on every insert
    update_buffer: Vec<Option<usize>>,

    // Keep a fast RNG as part of the struct
    rng: SmallRng,
}

impl SkipList {
    pub fn new(max_level: usize) -> Self {
        let head_node = Node {
            key: None,
            value: None,
            forward: vec![None; max_level + 1],
        };

        // Pre-allocate a decent capacity if you have a sense of how many inserts you’ll do.
        // e.g. `with_capacity(1000)`, or dynamic resizing strategy later
        let mut nodes = Vec::with_capacity(1000);
        nodes.push(head_node);

        SkipList {
            head: 0,
            nodes,
            max_level,
            current_level: 0,
            // Reusable buffer (max_level + 1)
            update_buffer: vec![None; max_level + 1],
            // Seed can be anything; for reproducibility, you might supply your own seed
            rng: SmallRng::from_entropy(),
        }
    }

    #[inline]
    fn random_level(&mut self) -> usize {
        let mut level = 0;
        // Probability 1/2 for stepping up the level
        while self.rng.gen_bool(0.5) && level < self.max_level {
            level += 1;
        }
        level
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SkipListError> {
        let level = self.random_level();
        debug!("Inserting key {:?} with level {}", key, level);

        // Instead of creating a new Vec on every insert, clear and reuse the buffer
        self.update_buffer.fill(None);

        let mut current = self.head;
        // Find the update path for each level (top-down)
        for i in (0..=self.current_level).rev() {
            while let Some(next_idx) = self.nodes[current].forward[i] {
                match self.nodes[next_idx].key.as_ref().unwrap().cmp(&key) {
                    Ordering::Less => current = next_idx,
                    Ordering::Equal => {
                        // If key already exists, just update the value
                        self.nodes[next_idx].value = Some(value);
                        return Ok(());
                    }
                    Ordering::Greater => break,
                }
            }
            self.update_buffer[i] = Some(current);
        }

        // Create new node
        let new_node = Node {
            key: Some(key.clone()),
            value: Some(value),
            forward: vec![None; level + 1],
        };

        // We can optionally reserve additional space if we anticipate growth
        if self.nodes.len() == self.nodes.capacity() {
            self.nodes.reserve(256);
        }
        let new_index = self.nodes.len();
        self.nodes.push(new_node);

        // Update forward pointers
        for i in 0..=level {
            let upd = self.update_buffer[i].unwrap_or(self.head);
            self.nodes[new_index].forward[i] = self.nodes[upd].forward[i];
            self.nodes[upd].forward[i] = Some(new_index);
        }

        // Update the current level if necessary
        if level > self.current_level {
            // The sentinel node must point to new_index on any newly added levels
            for i in (self.current_level + 1)..=level {
                self.nodes[self.head].forward[i] = Some(new_index);
            }
            self.current_level = level;
        }

        Ok(())
    }

    /// Retrieves a reference to the value associated with the given key in the skip list.
    ///
    /// This function performs a search through the skip list for the specified key.
    /// If the key exists, it returns a reference to the associated value.
    /// If the key is not found, it returns a [`SkipListError::KeyNotFound`] error.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key to search for in the skip list.
    ///
    /// # Returns
    ///
    /// * `Ok(&V)` - A reference to the value associated with the key.
    /// * `Err(SkipListError)` - An error if the key is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use kv_db::{SkipList, SkipListError};
    ///
    /// // Create a SkipList with some max_level (e.g. 5)
    /// let mut skip_list = SkipList::new(5);
    ///
    /// // Store both the key (i32) and the value (string) as bytes
    /// skip_list.put(
    ///     42i32.to_be_bytes().to_vec(),
    ///     b"Answer to everything".to_vec()
    /// ).unwrap();
    ///
    /// // Retrieve it using the same raw bytes
    /// match skip_list.get(42i32.to_be_bytes().to_vec()) {
    ///     Ok(value_bytes) => {
    ///         // Try interpreting the value as UTF-8
    ///         if let Ok(value_str) = String::from_utf8(value_bytes) {
    ///             println!("Found: {}", value_str);
    ///         } else {
    ///             println!("Found raw bytes, not valid UTF-8");
    ///         }
    ///     }
    ///     Err(SkipListError::KeyNotFound) => println!("Key not found."),
    /// }
    /// ```
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>, SkipListError> {
        let mut current = self.head;
        // we start the search at the highest level, and go down
        for level in (0..=self.current_level).rev() {
            while let Some(next_idx) = self.nodes[current].forward[level] {
                match self.nodes[next_idx].key.clone().unwrap().cmp(&key) {
                    // go to the next index
                    Ordering::Less => current = next_idx,
                    // we found the node
                    Ordering::Equal => {
                        return self.nodes[next_idx]
                            .value
                            .clone()
                            .ok_or(SkipListError::KeyNotFound);
                    }
                    // break out of the loop and go down a level
                    Ordering::Greater => break,
                }
            }
        }
        Err(SkipListError::KeyNotFound)
    }

    // Optional: For debug use only; remove or feature-gate to reduce overhead
    pub fn print_debug(&self) {
        debug!("SkipList state: current_level = {}", self.current_level);
        debug!("Total nodes = {}", self.nodes.len());
        for (i, node) in self.nodes.iter().enumerate() {
            if i == self.head {
                debug!("Head Node {}: forward={:?}", i, node.forward);
            } else {
                debug!(
                    "Node {}: key={:?}, value={:?}, forward={:?}",
                    i, node.key, node.value, node.forward
                );
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use env_logger::{Builder, Env};
    use rand::Rng;
    use std::collections::HashSet;

    // Helper function to initialize the logger once
    fn init_logger() {
        let _ = Builder::from_env(Env::default().default_filter_or("debug"))
            .is_test(true)
            .try_init();
    }

    #[test]
    fn test_insert_and_get_basic() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements with raw byte keys and values
        list.put(b"\x03".to_vec(), b"\x01\x02\x03".to_vec())
            .unwrap(); // Key: [0x03], Value: [0x01, 0x02, 0x03]
        list.put(b"\x01".to_vec(), b"\x04\x05\x06".to_vec())
            .unwrap(); // Key: [0x01], Value: [0x04, 0x05, 0x06]
        list.put(b"\x02".to_vec(), b"\x07\x08\x09".to_vec())
            .unwrap(); // Key: [0x02], Value: [0x07, 0x08, 0x09]

        // Verify insertion and retrieval
        assert_eq!(
            list.get(b"\x01".to_vec()).unwrap(),
            b"\x04\x05\x06".to_vec()
        );
        assert_eq!(
            list.get(b"\x02".to_vec()).unwrap(),
            b"\x07\x08\x09".to_vec()
        );
        assert_eq!(
            list.get(b"\x03".to_vec()).unwrap(),
            b"\x01\x02\x03".to_vec()
        );

        // Attempt to get a non-existent key
        assert!(list.get(b"\x04".to_vec()).is_err());
    }

    #[test]
    fn test_insert_sorted_order() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements in sorted order (keys are big-endian byte representations of 1..=10u32)
        for i in 1u32..=10u32 {
            let key = i.to_be_bytes().to_vec();
            let value = format!("number {}", i).into_bytes();
            list.put(key, value).unwrap();
        }

        // Verify all elements
        for i in 1u32..=10u32 {
            let key = i.to_be_bytes().to_vec();
            let expected = format!("number {}", i).into_bytes();
            assert_eq!(list.get(key).unwrap(), expected);
        }

        // Verify that non-existent key returns error
        assert!(list.get(11u32.to_be_bytes().to_vec()).is_err());
    }

    #[test]
    fn test_insert_reverse_order() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements in reverse order
        for i in (1u32..=10u32).rev() {
            let key = i.to_be_bytes().to_vec();
            let value = format!("number {}", i).into_bytes();
            list.put(key, value).unwrap();
        }

        // Verify all elements
        for i in 1u32..=10u32 {
            let key = i.to_be_bytes().to_vec();
            let expected = format!("number {}", i).into_bytes();
            assert_eq!(list.get(key).unwrap(), expected);
        }

        // Verify that non-existent key returns error
        assert!(list.get(0u32.to_be_bytes().to_vec()).is_err());
    }

    #[test]
    fn test_insert_duplicate_keys() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert a key
        list.put(1u32.to_be_bytes().to_vec(), b"one".to_vec())
            .unwrap();

        // Insert the same key with a different value
        list.put(1u32.to_be_bytes().to_vec(), b"uno".to_vec())
            .unwrap();

        // Verify that the value is updated
        assert_eq!(
            list.get(1u32.to_be_bytes().to_vec()).unwrap(),
            b"uno".to_vec()
        );
    }

    #[test]
    fn test_search_nonexistent_keys() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert some keys
        list.put(10u32.to_be_bytes().to_vec(), b"ten".to_vec())
            .unwrap();
        list.put(20u32.to_be_bytes().to_vec(), b"twenty".to_vec())
            .unwrap();
        list.put(30u32.to_be_bytes().to_vec(), b"thirty".to_vec())
            .unwrap();

        // Search for keys that do not exist
        let nonexistent_keys = vec![5u32, 15u32, 25u32, 35u32];

        for key_num in nonexistent_keys {
            let key = key_num.to_be_bytes().to_vec();
            assert!(
                list.get(key).is_err(),
                "Key {} should not be found",
                key_num
            );
        }
    }

    #[test]
    fn test_large_number_of_elements() {
        init_logger();

        let mut list = SkipList::new(16); // Higher max_level for larger lists
        let num_elements = 1000u32;
        let mut inserted_keys = HashSet::new();

        // Insert a large number of elements
        for i in 1u32..=num_elements {
            let key = i.to_be_bytes().to_vec();
            let value = format!("number {}", i).into_bytes();
            list.put(key, value).unwrap();
            inserted_keys.insert(i);
        }

        // Verify all inserted elements
        for key_num in inserted_keys.iter() {
            let key = key_num.to_be_bytes().to_vec();
            let expected_value = format!("number {}", key_num).into_bytes();
            assert_eq!(list.get(key).unwrap(), expected_value);
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![0u32, num_elements + 1, num_elements + 100];
        for key_num in nonexistent_keys {
            let key = key_num.to_be_bytes().to_vec();
            assert!(
                list.get(key).is_err(),
                "Key {} should not be found",
                key_num
            );
        }
    }

    #[test]
    fn test_random_insertions() {
        init_logger();

        let mut list = SkipList::new(16);
        let num_elements = 500;
        let mut rng = rand::thread_rng();
        let mut inserted_keys = HashSet::new();

        // Insert elements in random order
        for _ in 0..num_elements {
            // rng.gen_range(...) by default is i32; specify as u32 if you want to_be_bytes
            let key_num = rng.gen_range(1u32..=10000u32);
            let key = key_num.to_be_bytes().to_vec();
            let value = format!("value {}", key_num).into_bytes();
            list.put(key.clone(), value).unwrap();
            inserted_keys.insert(key_num);
        }

        // Verify all inserted elements
        for key_num in inserted_keys.iter() {
            let key = key_num.to_be_bytes().to_vec();
            let expected = format!("value {}", key_num).into_bytes();
            let retrieved = list.get(key).unwrap();
            assert_eq!(retrieved, expected);
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![10001u32, 20000u32, 30000u32];
        for key_num in nonexistent_keys {
            let key = key_num.to_be_bytes().to_vec();
            assert!(
                list.get(key).is_err(),
                "Key {} should not be found",
                key_num
            );
        }
    }

    #[test]
    fn test_max_level_elements() {
        init_logger();

        let max_level = 4;
        let mut list = SkipList::new(max_level);

        // Insert elements that potentially reach up to max_level
        for i in 1u32..=20u32 {
            let key = i.to_be_bytes().to_vec();
            let value = format!("number {}", i).into_bytes();
            list.put(key, value).unwrap();
        }

        // Verify all elements
        for i in 1u32..=20u32 {
            let key = i.to_be_bytes().to_vec();
            let expected = format!("number {}", i).into_bytes();
            assert_eq!(list.get(key).unwrap(), expected);
        }

        // Verify that non-existent keys return error
        assert!(list.get(0u32.to_be_bytes().to_vec()).is_err());
        assert!(list.get(21u32.to_be_bytes().to_vec()).is_err());
    }

    #[test]
    fn test_skiplist_integrity_after_multiple_insertions() {
        init_logger();

        let mut list = SkipList::new(10);
        let num_elements = 100;
        let mut rng = rand::thread_rng();
        let mut inserted_keys = Vec::new();

        // Insert elements in random order
        for _ in 0..num_elements {
            let key_num = rng.gen_range(1u32..=1000u32);
            let key = key_num.to_be_bytes().to_vec();
            let value = format!("value {}", key_num).into_bytes();
            list.put(key.clone(), value).unwrap();
            inserted_keys.push(key_num);
        }

        // Remove duplicates
        inserted_keys.sort_unstable();
        inserted_keys.dedup();

        // Verify all unique inserted elements
        for key_num in inserted_keys.iter() {
            let key = key_num.to_be_bytes().to_vec();
            let retrieved = list.get(key).unwrap();
            assert_eq!(retrieved, format!("value {}", key_num).into_bytes());
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![1001u32, 2000u32, 3000u32];
        for key_num in nonexistent_keys {
            let key = key_num.to_be_bytes().to_vec();
            assert!(
                list.get(key).is_err(),
                "Key {} should not be found",
                key_num
            );
        }
    }

    #[test]
    fn test_skiplist_empty() {
        init_logger();

        // An empty skiplist
        let list = SkipList::new(5);

        // Attempting to get any key should fail
        assert!(list.get(b"\x01".to_vec()).is_err());
        assert!(list.get(b"\x00".to_vec()).is_err());
        assert!(list.get(b"\xFF".to_vec()).is_err());
    }

    #[test]
    fn test_skiplist_single_element() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert a single element
        list.put(b"\x2A".to_vec(), b"forty-two".to_vec()).unwrap();

        // Verify the inserted element
        assert_eq!(list.get(b"\x2A".to_vec()).unwrap(), b"forty-two".to_vec());

        // Verify that other keys are not found
        assert!(list.get(b"\x29".to_vec()).is_err());
        assert!(list.get(b"\x2B".to_vec()).is_err());
    }

    #[test]
    fn test_skiplist_duplicate_inserts() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert the same key multiple times with different values
        list.put(b"\x64".to_vec(), b"hundred".to_vec()).unwrap();
        list.put(b"\x64".to_vec(), b"cien".to_vec()).unwrap();
        list.put(b"\x64".to_vec(), b"cent".to_vec()).unwrap();

        // Verify the latest value
        assert_eq!(list.get(b"\x64".to_vec()).unwrap(), b"cent".to_vec());
    }

    #[test]
    fn test_skiplist_forward_pointers_integrity() {
        init_logger();

        let mut list = SkipList::new(3);

        // Insert elements
        list.put(b"\x01".to_vec(), b"one".to_vec()).unwrap();
        list.put(b"\x02".to_vec(), b"two".to_vec()).unwrap();
        list.put(b"\x03".to_vec(), b"three".to_vec()).unwrap();
        list.put(b"\x04".to_vec(), b"four".to_vec()).unwrap();
        list.put(b"\x05".to_vec(), b"five".to_vec()).unwrap();

        // Traverse each level and verify sorted order
        for level in 0..=list.current_level {
            let mut current = list.head;
            let mut previous_key: Option<Vec<u8>> = None;

            while let Some(next_idx) = list.nodes[current].forward[level] {
                let node = &list.nodes[next_idx];
                let key = node.key.as_ref().unwrap();

                if let Some(ref prev) = previous_key {
                    // Ensure keys are in ascending byte order at this level
                    assert!(
                        prev < key,
                        "List is not sorted at level {} (prev={:?}, current={:?})",
                        level,
                        prev,
                        key
                    );
                }
                previous_key = Some(key.clone());
                current = next_idx;
            }
        }
    }
}
