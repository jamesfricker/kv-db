use rand::prelude::*;
use std::fmt::Debug;
use std::mem;

struct Node<T> {
    data: T,
    next: Option<Box<Node<T>>>,
}

struct SkipList<T: PartialOrd + Debug + Clone> {
    levels: Vec<LinkedList<T>>,
    max_level: usize,
}

impl<T: PartialOrd + Debug + Clone> SkipList<T> {
    fn new(max_level: usize) -> Self {
        let mut levels = Vec::with_capacity(max_level);
        for _ in 0..max_level {
            levels.push(LinkedList::new());
        }
        SkipList { levels, max_level }
    }

    fn insert(&mut self, data: T) {
        let mut rng = rand::thread_rng();
        let mut level = 0;
        let threshold = 0.5;

        while level < self.max_level && rng.gen::<f64>() < threshold {
            if level >= self.levels.len() {
                self.levels.push(LinkedList::new());
            }
            self.levels[level].push(data.clone());
            level += 1;
        }

        // Ensure the element is always inserted into the bottom level
        if level == 0 {
            self.levels[0].push(data);
        }
    }
    fn contains(&self, data: T) -> bool {
        if self.levels.is_empty() {
            return false;
        }

        let mut level = self.levels.len() - 1;
        let mut current = &self.levels[level].head;

        loop {
            while let Some(node) = current {
                if &node.data == &data {
                    return true;
                } else if &node.data > &data {
                    break;
                } else {
                    if let Some(_) = &node.next {
                        current = &node.next;
                    } else {
                        break;
                    }
                }
            }

            if level == 0 {
                return false;
            }

            // Move down to the next level
            level -= 1;
            let mut prev = &self.levels[level].head;
            while let Some(node) = prev {
                if &node.data >= &data {
                    break;
                }
                if let Some(next) = &node.next {
                    if &next.data > &data {
                        break;
                    }
                    prev = &node.next;
                } else {
                    break;
                }
            }
            current = prev;
        }
    }

    fn print(&self) {
        for (i, level) in self.levels.iter().enumerate() {
            print!("Level {}: ", i);
            level.print();
        }
    }
}

struct LinkedList<T> {
    head: Option<Box<Node<T>>>,
}

impl<T: PartialOrd + Debug> LinkedList<T> {
    // Create a new empty linked list
    fn new() -> Self {
        LinkedList { head: None }
    }

    // Add a new node to the list
    fn push(&mut self, data: T) {
        let mut curr = &mut self.head;

        loop {
            match curr {
                None => {
                    // We've reached the end of the list, insert here
                    *curr = Some(Box::new(Node { data, next: None }));
                    break;
                }
                Some(ref mut node) if node.data > data => {
                    // Found the correct position, insert before this node
                    let new_node = Box::new(Node {
                        data,
                        next: mem::replace(curr, None),
                    });
                    *curr = Some(new_node);
                    break;
                }
                Some(ref mut node) => {
                    // Keep moving to the next node
                    curr = &mut node.next;
                }
            }
        }
    }

    fn print(&self) {
        use std::fmt::Write;

        let mut print_string = String::new();
        let mut curr = &self.head;

        while let Some(node) = curr {
            // Use if statement to avoid writing the arrow for the last element
            if print_string.is_empty() {
                write!(print_string, "{:?}", node.data).unwrap();
            } else {
                write!(print_string, " -> {:?}", node.data).unwrap();
            }
            curr = &node.next;
        }

        println!("{}", print_string);
    }
}
fn main() {
    let mut skip_list = SkipList::new(5); // Create a skip list with 5 levels max
    let mut rng = rand::thread_rng();

    // Generate and insert 100 random numbers
    for _ in 0..100 {
        let number = rng.gen_range(0..=100);
        skip_list.insert(number);
    }

    // Print the skip list
    skip_list.print();

    println!("{}", skip_list.contains(55));
    println!("{}", skip_list.contains(40));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_list_is_empty() {
        let list: LinkedList<i32> = LinkedList::new();
        assert!(list.head.is_none());
    }

    #[test]
    fn test_push_and_order() {
        let mut list = LinkedList::new();
        list.push(3);
        list.push(1);
        list.push(4);
        list.push(2);

        let expected = vec![1, 2, 3, 4];
        assert_eq!(list_to_vec(&list), expected);
    }

    #[test]
    fn test_push_duplicates() {
        let mut list = LinkedList::new();
        list.push(1);
        list.push(1);
        list.push(2);
        list.push(1);

        let expected = vec![1, 1, 1, 2];
        assert_eq!(list_to_vec(&list), expected);
    }

    #[test]
    fn test_push_reverse_order() {
        let mut list = LinkedList::new();
        list.push(4);
        list.push(3);
        list.push(2);
        list.push(1);

        let expected = vec![1, 2, 3, 4];
        assert_eq!(list_to_vec(&list), expected);
    }

    // Helper function to convert LinkedList to Vec for easier testing
    fn list_to_vec<T: Clone>(list: &LinkedList<T>) -> Vec<T> {
        let mut result = Vec::new();
        let mut current = &list.head;
        while let Some(node) = current {
            result.push(node.data.clone());
            current = &node.next;
        }
        result
    }
}
