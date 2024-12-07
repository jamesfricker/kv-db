use std::fmt::Debug;
use std::mem;

struct Node<T> {
    data: T,
    next: Option<Box<Node<T>>>,
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
    let mut ll = LinkedList::new();
    ll.push(1);
    ll.push(4);
    ll.push(2);
    ll.push(3);
    ll.print();
}
