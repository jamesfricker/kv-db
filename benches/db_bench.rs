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

    // Initialize a new DB that expects raw bytes for key + value
    // (e.g. DB::new(path, max_level))
    let mut db = DB::new(wal_path.to_str().unwrap(), 10);
    let mut rng = rand::thread_rng();

    b.iter(|| {
        let key_i32 = rng.gen::<i32>();
        // Convert to big-endian bytes
        let key_bytes = key_i32.to_be_bytes().to_vec();
        // For the value, do the same (here we store the same i32 for simplicity)
        db.put(key_bytes.clone(), key_bytes).unwrap();
    });
}

#[bench]
fn bench_db_insert_existing(b: &mut Bencher) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    let mut db = DB::new(wal_path.to_str().unwrap(), 10);
    let mut rng = rand::thread_rng();

    // Pre-populate the DB with 1,000,000 elements
    // using random i32 -> bytes
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        db.put(key_bytes.clone(), key_bytes).unwrap();
    }

    b.iter(|| {
        // Insert additional elements
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        db.put(key_bytes.clone(), key_bytes).unwrap();
    });
}

#[bench]
fn bench_db_get_existing(b: &mut Bencher) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    let mut db = DB::new(wal_path.to_str().unwrap(), 10);
    let mut rng = rand::thread_rng();

    // We'll store the i32 keys in a Vec so we can retrieve them randomly
    let mut keys = Vec::with_capacity(1_000_000);

    // Pre-populate the DB with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        db.put(key_bytes.clone(), key_bytes.clone()).unwrap();
        keys.push(key_i32);
    }

    b.iter(|| {
        // Randomly select one of the known keys
        let index = rng.gen_range(0..keys.len());
        let key_i32 = keys[index];
        let key_bytes = key_i32.to_be_bytes().to_vec();
        let _ = db.get(key_bytes).unwrap(); // We expect success here
    });
}

#[bench]
fn bench_db_get_nonexistent(b: &mut Bencher) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    let mut db = DB::new(wal_path.to_str().unwrap(), 10);
    let mut rng = rand::thread_rng();

    // Pre-populate with 1,000,000 elements
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let key_bytes = key_i32.to_be_bytes().to_vec();
        db.put(key_bytes.clone(), key_bytes).unwrap();
    }

    b.iter(|| {
        // Generate a key that's unlikely to exist
        let missing_i32 = rng.gen::<i32>().wrapping_add(1_000_000);
        let missing_bytes = missing_i32.to_be_bytes().to_vec();
        let _ = db.get(missing_bytes);
    });
}
