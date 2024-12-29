// benches/bench_wal.rs

#![feature(test)]

extern crate test;

use kv_db::KvPair; // Adjust the path based on your project structure
use kv_db::Wal;
use rand::Rng;
use std::fs;
use tempfile::tempdir;
use test::Bencher;

/// Helper function to initialize a temporary WAL
fn setup_wal() -> (Wal, tempfile::TempDir) {
    let dir = tempdir().expect("Failed to create temp dir");
    let wal_path = dir.path().join("wal_bench.log");

    // Ensure the WAL file does not exist
    if wal_path.exists() {
        fs::remove_file(&wal_path).expect("Failed to remove existing WAL file");
    }

    // Initialize the WAL
    let wal = Wal::new(wal_path.to_str().unwrap().to_string()).expect("Failed to create WAL");

    (wal, dir)
}

/// Benchmark appending new `KvPair` entries to the WAL
#[bench]
fn bench_wal_append(b: &mut Bencher) {
    let (mut wal, _dir) = setup_wal();
    let kv = KvPair {
        key: "benchmark_key".to_string(),
        value: 42,
    };

    b.iter(|| {
        // Clone the KvPair to avoid borrowing issues
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
        let key = rng.gen::<i32>().to_string();
        let kv = KvPair {
            key,
            value: rng.gen::<i32>(),
        };
        wal.append(kv)
            .expect("Failed to append pre-populated KvPair");
    }

    // Prepare a KvPair for benchmarking
    let benchmark_kv = KvPair {
        key: "benchmark_key".to_string(),
        value: 42,
    };

    b.iter(|| {
        // Clone the KvPair to avoid borrowing issues
        wal.append(benchmark_kv.clone())
            .expect("Failed to append KvPair");
    });
}
