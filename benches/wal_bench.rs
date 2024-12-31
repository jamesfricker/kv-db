// benches/bench_wal.rs

#![feature(test)]

extern crate test;

use kv_db::{KvPair, Wal}; // Adjust the path if needed
use rand::Rng;
use std::fs;
use tempfile::{tempdir, TempDir};
use test::Bencher;

/// Helper function to initialize a temporary WAL
fn setup_wal() -> (Wal, TempDir) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize the WAL (assuming Wal::new takes a String path)
    let wal = Wal::new(wal_path.to_str().unwrap().to_string()).expect("Failed to create WAL");

    (wal, dir)
}

/// Benchmark appending new `KvPair` entries to the WAL
#[bench]
fn bench_wal_append(b: &mut Bencher) {
    let (mut wal, _dir) = setup_wal();

    // Example key = "benchmark_key", value = 42, both as raw bytes
    let kv = KvPair {
        key: b"benchmark_key".to_vec(),
        value: 42i32.to_be_bytes().to_vec(),
    };

    b.iter(|| {
        // `.clone()` because `append()` consumes the KvPair
        wal.append(kv.clone()).expect("Failed to append KvPair");
    });
}

/// Benchmark appending to an existing WAL with pre-populated entries
#[bench]
fn bench_wal_append_existing(b: &mut Bencher) {
    let (mut wal, _dir) = setup_wal();
    let mut rng = rand::thread_rng();

    // Pre-populate the WAL with 1,000,000 entries
    for _ in 0..1_000_000 {
        let key_i32 = rng.gen::<i32>();
        let val_i32 = rng.gen::<i32>();
        let kv = KvPair {
            key: key_i32.to_be_bytes().to_vec(),
            value: val_i32.to_be_bytes().to_vec(),
        };
        wal.append(kv)
            .expect("Failed to append pre-populated KvPair");
    }

    // Prepare a KvPair for benchmarking
    let benchmark_kv = KvPair {
        key: b"benchmark_key".to_vec(),
        value: 42i32.to_be_bytes().to_vec(),
    };

    b.iter(|| {
        wal.append(benchmark_kv.clone())
            .expect("Failed to append KvPair");
    });
}
