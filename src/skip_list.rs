pub use skip_list::SkipList;

mod skip_list {
    // Swap out rand::thread_rng with something faster, like fastrand or SmallRng
    // For demonstration, we'll use SmallRng from rand:
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
    pub struct Node<K, V> {
        pub key: Option<K>,
        value: Option<V>,
        pub forward: Vec<Option<usize>>,
    }

    pub struct SkipList<K, V> {
        pub head: usize, // Always points to the sentinel node
        pub nodes: Vec<Node<K, V>>,
        max_level: usize,
        pub current_level: usize,

        // Reusable vector to avoid re-allocating on every insert
        update_buffer: Vec<Option<usize>>,

        // Keep a fast RNG as part of the struct
        rng: SmallRng,
    }

    impl<K, V> SkipList<K, V>
    where
        K: Ord + Clone + Debug,
        V: Clone + Debug,
    {
        pub fn new(max_level: usize) -> Self {
            let head_node = Node {
                key: None,
                value: None,
                forward: vec![None; max_level + 1],
            };

            // Pre-allocate a decent capacity if you have a sense of how many inserts you’ll do.
            // e.g. `with_capacity(1000)`, or dynamic resizing strategy later
            let mut nodes = Vec::with_capacity(1);
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

        pub fn insert(&mut self, key: K, value: V) -> Result<(), SkipListError> {
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

        pub fn get(&self, key: &K) -> Result<&V, SkipListError> {
            let mut current = self.head;
            for level in (0..=self.current_level).rev() {
                while let Some(next_idx) = self.nodes[current].forward[level] {
                    match self.nodes[next_idx].key.as_ref().unwrap().cmp(key) {
                        Ordering::Less => current = next_idx,
                        Ordering::Equal => {
                            return self.nodes[next_idx]
                                .value
                                .as_ref()
                                .ok_or(SkipListError::KeyNotFound);
                        }
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
        // Initialize logger
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements
        list.insert(3, "three").unwrap();
        list.insert(1, "one").unwrap();
        list.insert(2, "two").unwrap();

        // Verify insertion and retrieval
        assert_eq!(list.get(&1).unwrap(), &"one");
        assert_eq!(list.get(&2).unwrap(), &"two");
        assert_eq!(list.get(&3).unwrap(), &"three");

        // Attempt to get a non-existent key
        assert!(list.get(&4).is_err());
    }

    #[test]
    fn test_insert_sorted_order() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements in sorted order
        for i in 1..=10 {
            list.insert(i, format!("number {}", i)).unwrap();
        }

        // Verify all elements
        for i in 1..=10 {
            assert_eq!(list.get(&i).unwrap(), &format!("number {}", i));
        }

        // Verify that non-existent key returns error
        assert!(list.get(&11).is_err());
    }

    #[test]
    fn test_insert_reverse_order() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert elements in reverse order
        for i in (1..=10).rev() {
            list.insert(i, format!("number {}", i)).unwrap();
        }

        // Verify all elements
        for i in 1..=10 {
            assert_eq!(list.get(&i).unwrap(), &format!("number {}", i));
        }

        // Verify that non-existent key returns error
        assert!(list.get(&0).is_err());
    }

    #[test]
    fn test_insert_duplicate_keys() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert a key
        list.insert(1, "one").unwrap();

        // Insert the same key with a different value
        list.insert(1, "uno").unwrap();

        // Verify that the value is updated
        assert_eq!(list.get(&1).unwrap(), &"uno");
    }

    #[test]
    fn test_search_nonexistent_keys() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert some keys
        list.insert(10, "ten").unwrap();
        list.insert(20, "twenty").unwrap();
        list.insert(30, "thirty").unwrap();

        // Search for keys that do not exist
        let nonexistent_keys = vec![5, 15, 25, 35];

        for key in nonexistent_keys {
            assert!(list.get(&key).is_err(), "Key {} should not be found", key);
        }
    }

    #[test]
    fn test_large_number_of_elements() {
        init_logger();

        let mut list = SkipList::new(16); // Higher max_level for larger lists
        let num_elements = 1000;
        let mut inserted_keys = HashSet::new();

        // Insert a large number of elements
        for i in 1..=num_elements {
            list.insert(i, format!("number {}", i)).unwrap();
            inserted_keys.insert(i);
        }

        // Verify all inserted elements
        for key in inserted_keys.iter() {
            assert_eq!(list.get(key).unwrap(), &format!("number {}", key));
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![0, num_elements + 1, num_elements + 100];
        for key in nonexistent_keys {
            assert!(list.get(&key).is_err(), "Key {} should not be found", key);
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
            let key = rng.gen_range(1..=10000);
            let value = format!("value {}", key);
            list.insert(key, value).unwrap();
            inserted_keys.insert(key);
        }

        // Verify all inserted elements
        for key in inserted_keys.iter() {
            let retrieved = list.get(key).unwrap();
            assert_eq!(retrieved, &format!("value {}", key));
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![10001, 20000, 30000];
        for key in nonexistent_keys {
            assert!(list.get(&key).is_err(), "Key {} should not be found", key);
        }
    }

    #[test]
    fn test_max_level_elements() {
        init_logger();

        let max_level = 4;
        let mut list = SkipList::new(max_level);

        // Insert elements that potentially reach up to max_level
        for i in 1..=20 {
            list.insert(i, format!("number {}", i)).unwrap();
        }

        // Verify all elements
        for i in 1..=20 {
            assert_eq!(list.get(&i).unwrap(), &format!("number {}", i));
        }

        // Verify that non-existent keys return error
        assert!(list.get(&0).is_err());
        assert!(list.get(&21).is_err());
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
            let key = rng.gen_range(1..=1000);
            let value = format!("value {}", key);
            list.insert(key, value).unwrap();
            inserted_keys.push(key);
        }

        // Remove duplicates
        inserted_keys.sort();
        inserted_keys.dedup();

        // Verify all unique inserted elements
        for key in inserted_keys.iter() {
            let retrieved = list.get(key).unwrap();
            assert_eq!(retrieved, &format!("value {}", key));
        }

        // Verify that non-existent keys return error
        let nonexistent_keys = vec![1001, 2000, 3000];
        for key in nonexistent_keys {
            assert!(list.get(&key).is_err(), "Key {} should not be found", key);
        }
    }

    #[test]
    fn test_skiplist_empty() {
        init_logger();

        let list: SkipList<i32, &str> = SkipList::new(5);

        // Attempt to get any key should fail
        assert!(list.get(&1).is_err());
        assert!(list.get(&0).is_err());
        assert!(list.get(&-1).is_err());
    }

    #[test]
    fn test_skiplist_single_element() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert a single element
        list.insert(42, "forty-two").unwrap();

        // Verify the inserted element
        assert_eq!(list.get(&42).unwrap(), &"forty-two");

        // Verify that other keys are not found
        assert!(list.get(&41).is_err());
        assert!(list.get(&43).is_err());
    }

    #[test]
    fn test_skiplist_duplicate_inserts() {
        init_logger();

        let mut list = SkipList::new(5);

        // Insert the same key multiple times with different values
        list.insert(100, "hundred").unwrap();
        list.insert(100, "cien").unwrap(); // Update
        list.insert(100, "cent").unwrap(); // Update again

        // Verify the latest value
        assert_eq!(list.get(&100).unwrap(), &"cent");
    }

    #[test]
    fn test_skiplist_forward_pointers_integrity() {
        init_logger();

        let mut list = SkipList::new(3);

        // Insert elements
        list.insert(1, "one").unwrap();
        list.insert(2, "two").unwrap();
        list.insert(3, "three").unwrap();
        list.insert(4, "four").unwrap();
        list.insert(5, "five").unwrap();

        // Traverse each level and verify sorted order
        for level in 0..=list.current_level {
            let mut current = list.head;
            let mut previous_key = None;

            while let Some(next_idx) = list.nodes[current].forward[level] {
                let node = &list.nodes[next_idx];
                let key = node.key.as_ref().unwrap();

                if let Some(prev) = previous_key {
                    assert!(prev < key, "List is not sorted at level {}", level);
                }

                previous_key = Some(key);
                current = next_idx;
            }
        }
    }
}
