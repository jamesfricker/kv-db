// Ensure you are using Nightly Rust to enable the `test` feature
#![feature(test)]

extern crate test;

use kv_db::db::DB;
use rand::Rng;
use std::fs;
use tempfile::tempdir;
use test::Bencher;

#[bench]
fn bench_db_insert(b: &mut Bencher) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize a new Database with key and value as i32
    let mut db: DB<i32, i32> = DB::new(&wal_path.to_str().unwrap().to_string(), 10);
    let mut rng = rand::thread_rng();

    b.iter(|| {
        // Generate a random key and use it as the value for simplicity
        let key = rng.gen::<i32>();
        db.put(key, key).unwrap();
    });
}

#[bench]
fn bench_db_insert_existing(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize a new Database with key and value as i32
    let mut db: DB<i32, i32> = DB::new(&wal_path.to_str().unwrap().to_string(), 10);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        db.put(key, key).unwrap();
    }

    b.iter(|| {
        // Insert additional elements
        let key = rng.gen::<i32>();
        db.put(key, key).unwrap();
    });
}

#[bench]
fn bench_db_get_existing(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize a new Database with key and value as i32
    let mut db: DB<i32, i32> = DB::new(&wal_path.to_str().unwrap().to_string(), 10);
    let mut rng = rand::thread_rng();
    let mut keys = Vec::with_capacity(1_000_000);

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        db.put(key, key).unwrap();
        keys.push(key);
    }

    b.iter(|| {
        // Randomly select a key to retrieve
        let index = rng.gen_range(0..keys.len());
        let key = keys[index];
        db.get(&key).unwrap();
    });
}

#[bench]
fn bench_db_get_nonexistent(b: &mut Bencher) {
    // Initialize a new SkipList with key and value as i32
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize a new Database with key and value as i32
    let mut db: DB<i32, i32> = DB::new(&wal_path.to_str().unwrap().to_string(), 10);
    let mut rng = rand::thread_rng();

    // Pre-populate the skip list with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key = rng.gen::<i32>();
        db.put(key, key).unwrap();
    }

    b.iter(|| {
        // Generate a key that is unlikely to exist
        let key = rng.gen::<i32>() + 1_000_000;
        let _ = db.get(&key);
    });
}
