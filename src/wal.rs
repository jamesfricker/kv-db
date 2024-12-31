// --------------- wal.rs ---------------
use crate::kv::KvPair;
use bincode::{deserialize, serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};

/// Write-Ahead Log
///
/// Persists key/value pairs in a length-prefixed bincode format:
/// [4-byte big-endian length] [bincode-serialized KvPair].
pub struct Wal {
    location: String,
    file: File,
}

impl Wal {
    /// Creates a new `Wal` instance, creating the file if it doesn't exist.
    /// Opens the file for reading and appending.
    pub fn new(location: String) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&location)?;

        Ok(Wal { location, file })
    }

    /// Appends a single key-value record (as raw bytes) to the WAL.
    ///
    /// 1. We bincode-serialize the `KvPair` (which already has `Vec<u8>` key + `Vec<u8>` value).
    /// 2. We write a 4-byte length (big-endian).
    /// 3. We write the bytes themselves.
    /// 4. We flush to ensure durability.
    pub fn append(&mut self, kv: KvPair) -> io::Result<()> {
        let serialized = serialize(&kv).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let record_len = serialized.len() as u32;
        // Write length prefix
        self.file.write_all(&record_len.to_be_bytes())?;
        // Write the actual record
        self.file.write_all(&serialized)?;
        self.file.flush()?;

        Ok(())
    }

    /// Reads *all* records from the WAL as `KvPair` (raw bytes for key + value).
    /// On EOF, it returns all records read so far.
    pub fn read(&self) -> io::Result<Vec<KvPair>> {
        let file = File::open(&self.location)?;
        let mut reader = BufReader::new(file);

        let mut kv_pairs = Vec::new();

        loop {
            // Read the 4-byte length
            let mut len_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut len_buf) {
                // If it's EOF, we're done
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }

            // Convert length to usize
            let record_len = u32::from_be_bytes(len_buf) as usize;
            let mut data = vec![0u8; record_len];
            reader.read_exact(&mut data)?;

            let kv = deserialize(&data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            kv_pairs.push(kv);
        }

        Ok(kv_pairs)
    }

    /// Returns the raw (serialized) records as `Vec<Vec<u8>>`.
    /// Each record is just the bincode payload (no 4-byte prefix).
    pub fn read_raw(&self) -> io::Result<Vec<Vec<u8>>> {
        let file = File::open(&self.location)?;
        let mut reader = BufReader::new(file);

        let mut raw_records = Vec::new();
        loop {
            // Read 4-byte length
            let mut len_buf = [0; 4];
            if let Err(e) = reader.read_exact(&mut len_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break; // stop at EOF
                } else {
                    return Err(e);
                }
            }
            let record_len = u32::from_be_bytes(len_buf) as usize;

            // Read `record_len` bytes
            let mut data = vec![0; record_len];
            reader.read_exact(&mut data)?;

            // Store this binary chunk as-is
            raw_records.push(data);
        }

        Ok(raw_records)
    }
}

// --------------- tests.rs ---------------
#[cfg(test)]
mod tests {
    use super::Wal;
    use crate::kv::KvPair;

    use bincode;
    use env_logger::{Builder, Env};
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tempfile::NamedTempFile;

    fn init_logger() {
        let _ = Builder::from_env(Env::default().default_filter_or("debug"))
            .is_test(true)
            .try_init();
    }

    ///     1) Write a bool KV
    ///     2) Write a float KV
    ///     3) Write an i32 KV
    /// Then read them all back using `read_raw()`, which gives us raw bytes,
    /// and manually deserialize them with `bincode::deserialize`.
    #[test]
    fn test_wal_raw_bytes() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        {
            let mut w = Wal::new(path.clone())?;

            // Key = "flag", value = true (serialized)
            w.append(KvPair::new(
                b"flag".to_vec(),
                bincode::serialize(&true).unwrap(),
            ))?;

            // Key = "pi", value = f64::consts::PI (serialized)
            w.append(KvPair::new(
                b"pi".to_vec(),
                bincode::serialize(&std::f64::consts::PI).unwrap(),
            ))?;

            // Key = "hello", value = 42_i32 (serialized)
            w.append(KvPair::new(
                b"hello".to_vec(),
                bincode::serialize(&42_i32).unwrap(),
            ))?;
        }

        // Now read the *raw* bytes
        let w = Wal::new(path)?;
        let raw_records = w.read_raw()?;

        assert_eq!(raw_records.len(), 3);

        // Manually decode each record
        let kv_bool: KvPair = bincode::deserialize(&raw_records[0]).unwrap();
        assert_eq!(kv_bool.key, b"flag".to_vec());
        let bool_val: bool = bincode::deserialize(&kv_bool.value).unwrap();
        assert!(bool_val);

        let kv_float: KvPair = bincode::deserialize(&raw_records[1]).unwrap();
        assert_eq!(kv_float.key, b"pi".to_vec());
        let pi_val: f64 = bincode::deserialize(&kv_float.value).unwrap();
        assert!((pi_val - std::f64::consts::PI).abs() < f64::EPSILON);

        let kv_int: KvPair = bincode::deserialize(&raw_records[2]).unwrap();
        assert_eq!(kv_int.key, b"hello".to_vec());
        let int_val: i32 = bincode::deserialize(&kv_int.value).unwrap();
        assert_eq!(int_val, 42);

        Ok(())
    }

    /// Tests that multiple WAL instances point to different files and do not interfere.
    #[test]
    fn test_multiple_wal_instances() -> io::Result<()> {
        init_logger();

        let temp1 = NamedTempFile::new()?;
        let temp2 = NamedTempFile::new()?;

        let p1 = temp1.path().to_string_lossy().to_string();
        let p2 = temp2.path().to_string_lossy().to_string();

        let mut w1 = Wal::new(p1.clone())?;
        let mut w2 = Wal::new(p2.clone())?;

        // In w1, key="w1-a", value=1
        w1.append(KvPair::new(
            b"w1-a".to_vec(),
            bincode::serialize(&1i32).unwrap(),
        ))?;
        // In w2, key="w2-b", value=2
        w2.append(KvPair::new(
            b"w2-b".to_vec(),
            bincode::serialize(&2i32).unwrap(),
        ))?;

        // Reopen them
        let w1b = Wal::new(p1)?;
        let w2b = Wal::new(p2)?;

        // Read all KvPairs
        let raw1 = w1b.read()?;
        let raw2 = w2b.read()?;
        assert_eq!(raw1.len(), 1, "w1 should have 1 record");
        assert_eq!(raw2.len(), 1, "w2 should have 1 record");

        // Decode the values
        let val1: i32 = bincode::deserialize(&raw1[0].value).unwrap();
        assert_eq!(raw1[0].key, b"w1-a".to_vec());
        assert_eq!(val1, 1);

        let val2: i32 = bincode::deserialize(&raw2[0].value).unwrap();
        assert_eq!(raw2[0].key, b"w2-b".to_vec());
        assert_eq!(val2, 2);

        Ok(())
    }

    /// Appends a large number of records (e.g., 20,000) to test performance/correctness.
    #[test]
    fn test_stress_appends() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        let mut w = Wal::new(path.clone())?;

        let total_records = 20_000;
        for i in 0..total_records {
            let key = format!("k{}", i).into_bytes();
            let value = bincode::serialize(&(i as i32)).unwrap(); // store i32
            w.append(KvPair::new(key, value))?;
        }

        // Reopen & read
        let w = Wal::new(path)?;
        let all = w.read()?;
        assert_eq!(
            all.len(),
            total_records,
            "Mismatch in record count after stress test"
        );

        // Spot check a few entries
        // the first
        assert_eq!(all[0].key, b"k0".to_vec());
        let val0: i32 = bincode::deserialize(&all[0].value).unwrap();
        assert_eq!(val0, 0);

        // the last
        let last = &all[all.len() - 1];
        let val_last: i32 = bincode::deserialize(&last.value).unwrap();
        assert_eq!(last.key, format!("k{}", total_records - 1).into_bytes());
        assert_eq!(val_last, (total_records - 1) as i32);

        Ok(())
    }

    /// Test writing and then reading back a very large single record (~1 MB).
    #[test]
    fn test_large_record() -> io::Result<()> {
        init_logger();

        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        let mut w = Wal::new(path.clone())?;

        // Create a big string of ~1MB
        let size = 1_000_000;
        let big_string = "x".repeat(size);
        let kv = KvPair::new(
            b"big_record".to_vec(),
            bincode::serialize(&big_string).unwrap(),
        );
        w.append(kv)?;

        // Reopen & read
        let w = Wal::new(path)?;
        let all = w.read()?;
        assert_eq!(all.len(), 1, "Expected exactly one record");
        assert_eq!(all[0].key, b"big_record".to_vec());

        let read_back: String = bincode::deserialize(&all[0].value).unwrap();
        assert_eq!(read_back.len(), size, "Expected data length to match");
        Ok(())
    }

    /// Test reading from the WAL after reopening it multiple times.
    #[test]
    fn test_multiple_reopens() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        {
            // Round 1: create WAL, append 5 records
            let mut w = Wal::new(path.clone())?;
            for i in 0..5 {
                let key = format!("round1-{}", i).into_bytes();
                let val = bincode::serialize(&i).unwrap();
                w.append(KvPair::new(key, val))?;
            }
        }

        {
            // Round 2: reopen, append 5 more
            let mut w = Wal::new(path.clone())?;
            for i in 5..10 {
                let key = format!("round2-{}", i).into_bytes();
                let val = bincode::serialize(&i).unwrap();
                w.append(KvPair::new(key, val))?;
            }
        }

        // Final read
        let w = Wal::new(path)?;
        let all = w.read()?;
        assert_eq!(all.len(), 10, "Expected total of 10 records");

        // Check keys
        for (count, kvp) in all.into_iter().enumerate() {
            let key_s = String::from_utf8_lossy(&kvp.key);
            if count < 5 {
                assert_eq!(key_s, format!("round1-{}", count));
            } else {
                assert_eq!(key_s, format!("round2-{}", count));
            }
        }

        Ok(())
    }

    /// Simulate truncating the WAL in the middle of a record (like a crash).
    /// The final record can't be fully read. We check if it's ignored or triggers an error.
    #[test]
    fn test_truncated_record() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        // Write 2 complete records
        {
            let mut w = Wal::new(path.clone())?;
            w.append(KvPair::new(
                b"complete1".to_vec(),
                bincode::serialize(&1_i32).unwrap(),
            ))?;
            w.append(KvPair::new(
                b"complete2".to_vec(),
                bincode::serialize(&2_i32).unwrap(),
            ))?;
        }

        // Now write half of a third record's bytes
        {
            let kv = KvPair::new(b"partial".to_vec(), bincode::serialize(&999_i32).unwrap());
            let serialized = bincode::serialize(&kv).unwrap();
            let record_len = serialized.len() as u32;

            // manually open the file in append mode
            let mut f = std::fs::OpenOptions::new().append(true).open(&path)?;
            // write the 4-byte length
            f.write_all(&record_len.to_be_bytes())?;
            // write only half the data
            let half = serialized.len() / 2;
            f.write_all(&serialized[..half])?;
            f.flush()?;
        }

        // Now read from the WAL
        let w = Wal::new(path)?;
        let result = w.read();

        match result {
            Ok(records) => {
                // Code that ignores partial record will just return the complete ones
                assert_eq!(records.len(), 2, "Expected only 2 valid records");
            }
            Err(e) => {
                // Or your code might fail when it sees incomplete data
                eprintln!("Got an error reading partial record: {}", e);
            }
        }

        Ok(())
    }

    /// Manually corrupt one of the records in the middle to ensure that only that record fails,
    /// or the whole read fails, depending on your design.
    #[test]
    fn test_partial_corruption_in_the_middle() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        // Write 3 valid records
        {
            let mut w = Wal::new(path.clone())?;
            for i in 0..3 {
                let kv = KvPair::new(
                    format!("pre_corrupt_{}", i).into_bytes(),
                    bincode::serialize(&i).unwrap(),
                );
                w.append(kv)?;
            }
        }

        // Corrupt the middle record’s data in the file
        {
            let mut contents = Vec::new();
            {
                let mut f = std::fs::File::open(&path)?;
                f.read_to_end(&mut contents)?;
            }

            // We have 3 records => 3 length prefixes + 3 data blobs
            // We'll skip the first record, then corrupt part of the second.
            let mut idx = 0;
            for _rec_idx in 0..1 {
                // skip length + data for the first record
                if idx + 4 > contents.len() {
                    break;
                }
                let len_buf = &contents[idx..idx + 4];
                let record_len = u32::from_be_bytes(len_buf.try_into().unwrap()) as usize;
                idx += 4 + record_len;
            }

            // Now idx should be at the start of the 2nd record’s length prefix
            // move ahead 4 bytes to get to its data
            if idx + 4 <= contents.len() {
                idx += 4;
                // Now idx is at the start of the actual record bytes
                // Let's corrupt 5 bytes
                let end_idx = (idx + 5).min(contents.len());
                for byte in contents.iter_mut().skip(idx).take(end_idx - idx) {
                    *byte = 0xFF;
                }
            }

            // Write it back
            {
                let mut f = std::fs::File::create(&path)?;
                f.write_all(&contents)?;
            }
        }

        // Now read
        let w = Wal::new(path)?;
        let res = w.read();

        match res {
            Ok(records) => {
                // Possibly you only read the first record successfully, then error on the 2nd,
                // or skip the corrupted record. Implementation depends on your design.
                println!(
                    "Records read successfully (some might be missing): {:?}",
                    records
                );
            }
            Err(e) => {
                // Possibly you fail as soon as you hit the corrupted record.
                println!("Got an error reading corrupted record: {}", e);
            }
        }

        Ok(())
    }

    /// Very simplistic concurrency test: multiple threads each append multiple records.
    /// We wrap the single WAL in a Mutex so that writes do not interleave arbitrarily.
    #[test]
    fn test_concurrent_appends() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        let wal = Arc::new(Mutex::new(Wal::new(path.clone())?));

        let thread_count = 4;
        let writes_per_thread = 50;
        let mut handles = Vec::with_capacity(thread_count);

        for t_id in 0..thread_count {
            let wal_clone = Arc::clone(&wal);
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("t{}-k{}", t_id, i).into_bytes();
                    let value = bincode::serialize(&(i as i32)).unwrap();
                    let mut locked = wal_clone.lock().unwrap();
                    locked
                        .append(KvPair::new(key, value))
                        .expect("append failed");
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Now read them all
        let final_wal = Wal::new(path)?;
        let all = final_wal.read()?;
        assert_eq!(
            all.len(),
            thread_count * writes_per_thread,
            "Mismatch in total records after concurrency test"
        );

        Ok(())
    }
}
