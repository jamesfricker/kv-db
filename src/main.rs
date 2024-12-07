use rand::Rng;
use std::fmt::Debug;

#[derive(Clone)]
struct Node<T> {
    data: T,
    next: Option<usize>,
}

struct SkipList<T> {
    nodes: Vec<Node<T>>,
    heads: Vec<Option<usize>>,
    max_level: usize,
}

impl<T: PartialOrd + Debug + Clone> SkipList<T> {
    fn new(max_level: usize) -> Self {
        SkipList {
            nodes: Vec::new(),
            heads: vec![None; max_level],
            max_level,
        }
    }

    fn insert(&mut self, data: T) {
        let mut rng = rand::thread_rng();
        let mut level = 0;

        while level < self.max_level && rng.gen::<f64>() < 0.5 {
            level += 1;
        }

        let mut prev = vec![None; level];
        let mut current_index = if level > 0 {
            self.heads[level - 1]
        } else {
            None
        };

        for i in (0..level).rev() {
            loop {
                match current_index {
                    Some(idx) if self.nodes[idx].data <= data => {
                        prev[i] = current_index;
                        current_index = self.nodes[idx].next;
                    }
                    _ => break,
                }
            }

            if i > 0 {
                current_index = self.heads[i - 1];
            }
        }

        for i in 0..level {
            let new_index = self.nodes.len();
            self.nodes.push(Node {
                data: data.clone(),
                next: None,
            });

            if let Some(prev_index) = prev[i] {
                self.nodes[prev_index].next = Some(new_index);
            } else {
                self.heads[i] = Some(new_index);
            }
        }
    }

    fn contains(&self, data: &T) -> bool {
        let mut level = self.max_level;
        let mut current_index = None;

        while level > 0 {
            level -= 1;
            current_index = self.heads[level];

            while let Some(idx) = current_index {
                match self.nodes[idx].data.partial_cmp(data) {
                    Some(std::cmp::Ordering::Equal) => return true,
                    Some(std::cmp::Ordering::Greater) => break,
                    _ => {
                        current_index = self.nodes[idx].next;
                    }
                }
            }
        }

        false
    }

    fn print(&self) {
        for (i, &head) in self.heads.iter().enumerate() {
            print!("Level {}: ", i);
            let mut current = head;
            while let Some(idx) = current {
                print!("{:?} -> ", self.nodes[idx].data);
                current = self.nodes[idx].next;
            }
            println!("None");
        }
    }
}

fn main() {
    let mut skip_list: SkipList<i32> = SkipList::new(5);

    for i in 0..20 {
        skip_list.insert(i);
    }

    skip_list.print();

    println!("Contains 10: {}", skip_list.contains(&10));
    println!("Contains 20: {}", skip_list.contains(&20));
}
