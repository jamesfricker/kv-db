// Ensure you are using Nightly Rust to enable the `test` feature
#![feature(test)]

extern crate test;

use kv_db::SkipList;
use rand::Rng;
use test::Bencher;

#[bench]
fn bench_insert(b: &mut Bencher) {
    // Initialize a new SkipList with raw byte keys/values
    let mut skip_list = SkipList::new(20);
    let mut rng = rand::thread_rng();

    b.iter(|| {
        // Generate a random i32 and convert to bytes
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();

        // For simplicity, store the same data as the "value"
        skip_list.put(key_bytes.clone(), key_bytes).unwrap();
    });
}

#[bench]
fn bench_insert_existing(b: &mut Bencher) {
    let mut skip_list = SkipList::new(20);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        skip_list.put(key_bytes.clone(), key_bytes).unwrap();
    }

    b.iter(|| {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        skip_list.put(key_bytes.clone(), key_bytes).unwrap();
    });
}

#[bench]
fn bench_get_existing(b: &mut Bencher) {
    let mut skip_list = SkipList::new(20);
    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1_000_000);

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        skip_list.put(key_bytes.clone(), key_bytes.clone()).unwrap();
        keys.push(key_i32); // Store the original i32 so we can retrieve it randomly
    }

    b.iter(|| {
        // Randomly select a key to retrieve
        let index = rng.gen_range(0..keys.len());
        let key_i32 = keys[index];
        let key_bytes = key_i32.to_be_bytes().to_vec();
        skip_list.get(key_bytes).unwrap();
    });
}

#[bench]
fn bench_get_nonexistent(b: &mut Bencher) {
    let mut skip_list = SkipList::new(20);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        skip_list.put(key_bytes.clone(), key_bytes).unwrap();
    }

    b.iter(|| {
        // Generate a key that is unlikely to exist
        let key_i32 = rng.gen::<i32>().wrapping_add(1_000_000);
        let key_bytes = key_i32.to_be_bytes().to_vec();
        let _ = skip_list.get(key_bytes);
    });
}
