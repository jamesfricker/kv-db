// Ensure you are using Nightly Rust to enable the `test` feature
#![feature(test)]

extern crate test;

use kv_db::SkipList;
use rand::Rng;
use test::Bencher;

#[bench]
fn bench_insert(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let mut skip_list: SkipList<i32, i32> = SkipList::new(20);
    let mut rng = rand::thread_rng();

    b.iter(|| {
        // Generate a random key and use it as the value for simplicity
        let key = rng.gen::<i32>();
        skip_list.put(key, key).unwrap();
    });
}

#[bench]
fn bench_insert_existing(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let mut skip_list: SkipList<i32, i32> = SkipList::new(20);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        skip_list.put(key, key).unwrap();
    }

    b.iter(|| {
        // Insert additional elements
        let key = rng.gen::<i32>();
        skip_list.put(key, key).unwrap();
    });
}

#[bench]
fn bench_get_existing(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let mut skip_list: SkipList<i32, i32> = SkipList::new(20);
    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1_000_000);

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        skip_list.put(key, key).unwrap();
        keys.push(key);
    }

    b.iter(|| {
        // Randomly select a key to retrieve
        let index = rng.gen_range(0..keys.len());
        let key = keys[index];
        skip_list.get(&key).unwrap();
    });
}

#[bench]
fn bench_get_nonexistent(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let mut skip_list: SkipList<i32, i32> = SkipList::new(20);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        skip_list.put(key, key).unwrap();
    }

    b.iter(|| {
        // Generate a key that is unlikely to exist
        let key = rng.gen::<i32>() + 1_000_000;
        let _ = skip_list.get(&key);
    });
}
