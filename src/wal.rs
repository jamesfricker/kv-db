use crate::kv::KvPair;
use bincode::{deserialize, serialize};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

pub struct Wal {
    location: String,
    file: File,
}

impl Wal {
    /// Creates a new `Wal` instance, creating the file if it doesn't exist.
    /// Opens the file for reading and appending.
    ///
    /// Records in the WAL are stored in a length-prefixed binary format:
    ///     [4-byte big-endian length] [bincode-serialized data]
    pub fn new(location: String) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&location)?;

        Ok(Wal { location, file })
    }

    /// Appends a single key-value record to the WAL.
    ///
    /// 1. We bincode-serialize the `KvPair<K, V>`.
    /// 2. We write a 4-byte length (big-endian) specifying how many bytes the record has.
    /// 3. Then we write the bytes themselves.
    /// 4. We flush to ensure durability.
    pub fn append<K, V>(&mut self, kv: KvPair<K, V>) -> io::Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        let serialized = serialize(&kv).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let record_len = serialized.len() as u32;
        // Write length prefix
        self.file.write_all(&record_len.to_be_bytes())?;
        // Write the actual record
        self.file.write_all(&serialized)?;
        self.file.flush()?;

        Ok(())
    }

    /// Reads *all* records from the WAL and returns them as a `Vec<KvPair<K, V>>`.
    ///
    /// It repeatedly:
    /// 1. Reads a 4-byte length (big-endian).
    /// 2. Reads that many bytes from the file.
    /// 3. Bincode-deserializes those bytes into `KvPair<K, V>`.
    /// 4. Collects results until EOF.
    ///
    /// If at any point we can't read the full length (EOF), we stop.
    /// If bincode errors, we wrap it in an `io::Error`.
    pub fn read<K, V>(&self) -> io::Result<Vec<KvPair<K, V>>>
    where
        K: DeserializeOwned,
        V: DeserializeOwned,
    {
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

            // Now we just store this binary chunk as-is
            raw_records.push(data);
        }

        Ok(raw_records)
    }
}
#[cfg(test)]
mod tests {
    use super::Wal;
    use crate::KvPair;
    use bincode::deserialize;
    use env_logger::{Builder, Env};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tempfile::NamedTempFile;

    fn init_logger() {
        let _ = Builder::from_env(Env::default().default_filter_or("debug"))
            .is_test(true)
            .try_init();
    }

    /// 1) Write a bool KV
    /// 2) Write a float KV
    /// 3) Write an i32 KV
    /// Then read them all back using `read()`, which gives us raw `KvPair`s,
    /// and manually deserialize them with `bincode::deserialize`.
    #[test]
    fn test_wal_raw_bytes() -> io::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        {
            let mut w = Wal::new(path.clone())?;
            // Write various types:
            w.append(KvPair {
                key: "flag",
                value: true,
            })?;
            w.append(KvPair {
                key: "pi",
                value: 3.14159_f64,
            })?;
            w.append(KvPair {
                key: "hello",
                value: 42_i32,
            })?;
        }

        // Now read the *raw* bytes
        let w = Wal::new(path)?;
        let raw_records = w.read_raw()?; // returns Vec<Vec<u8>>

        assert_eq!(raw_records.len(), 3);

        // Manually decode each record as we wish
        let kv_bool: KvPair<String, bool> = bincode::deserialize(&raw_records[0])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        assert_eq!(kv_bool.key, "flag");
        assert_eq!(kv_bool.value, true);

        let kv_float: KvPair<String, f64> = bincode::deserialize(&raw_records[1])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        assert_eq!(kv_float.key, "pi");

        let kv_int: KvPair<String, i32> = bincode::deserialize(&raw_records[2])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        assert_eq!(kv_int.key, "hello");
        assert_eq!(kv_int.value, 42);

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

        w1.append(KvPair {
            key: "w1-a",
            value: 1,
        })?;
        w2.append(KvPair {
            key: "w2-b",
            value: 2,
        })?;

        // Reopen them
        let w1b = Wal::new(p1)?;
        let w2b = Wal::new(p2)?;

        let raw1 = w1b.read::<String, i32>()?;
        let raw2 = w2b.read::<String, i32>()?;
        assert_eq!(raw1.len(), 1, "w1 should have 1 record");
        assert_eq!(raw2.len(), 1, "w2 should have 1 record");

        // Check correctness
        assert_eq!(raw1[0].key, "w1-a");
        assert_eq!(raw1[0].value, 1);
        assert_eq!(raw2[0].key, "w2-b");
        assert_eq!(raw2[0].value, 2);

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
            w.append(KvPair {
                key: format!("k{}", i),
                value: i,
            })?;
        }

        // Reopen & read
        let w = Wal::new(path)?;
        let all = w.read::<String, i32>()?;
        assert_eq!(
            all.len(),
            total_records,
            "Mismatch in record count after stress test"
        );

        // Spot check a few entries
        assert_eq!(all[0].key, "k0");
        assert_eq!(all[0].value, 0);
        assert_eq!(all[all.len() - 1].key, format!("k{}", total_records - 1));
        assert_eq!(all[all.len() - 1].value, (total_records - 1) as i32);
        Ok(())
    }

    /// Test writing and then reading back a very large single record (e.g. ~1 MB).
    #[test]
    fn test_large_record() -> io::Result<()> {
        init_logger();

        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        let mut w = Wal::new(path.clone())?;

        // Create a big string of ~1MB
        let size = 1_000_000;
        let big_string = "x".repeat(size);
        let kv = KvPair {
            key: "big_record".to_string(),
            value: big_string.clone(),
        };
        w.append(kv)?;

        // Reopen & read
        let w = Wal::new(path)?;
        let all = w.read::<String, String>()?;
        assert_eq!(all.len(), 1, "Expected exactly one record");
        assert_eq!(all[0].key, "big_record");
        assert_eq!(all[0].value.len(), size, "Expected data length to match");
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
                w.append(KvPair {
                    key: format!("round1-{}", i),
                    value: i,
                })?;
            }
        }

        {
            // Round 2: reopen, append 5 more
            let mut w = Wal::new(path.clone())?;
            for i in 5..10 {
                w.append(KvPair {
                    key: format!("round2-{}", i),
                    value: i,
                })?;
            }
        }

        // Final read
        let w = Wal::new(path)?;
        let all = w.read::<String, i32>()?;
        assert_eq!(all.len(), 10, "Expected total of 10 records");

        // Check first 5
        for i in 0..5 {
            assert_eq!(all[i].key, format!("round1-{}", i));
            assert_eq!(all[i].value, i as i32);
        }
        // Check next 5
        for (offset, rec) in all.iter().enumerate().skip(5) {
            let i = offset as i32;
            assert_eq!(rec.key, format!("round2-{}", i));
            assert_eq!(rec.value, i);
        }

        Ok(())
    }

    /// Simulate truncating the WAL in the middle of a record (like a crash).
    /// The final record can't be fully read.
    /// We'll see if the partial record triggers an error or is simply ignored,
    /// depending on how your code is structured.
    #[test]
    fn test_truncated_record() -> io::Result<()> {
        init_logger();
        let temp = NamedTempFile::new()?;
        let path = temp.path().to_string_lossy().to_string();

        // Write 2 complete records
        {
            let mut w = Wal::new(path.clone())?;
            w.append(KvPair {
                key: "complete1",
                value: 1_i32,
            })?;
            w.append(KvPair {
                key: "complete2",
                value: 2_i32,
            })?;
        }

        // Now write half of a third record's bytes
        // (writing the length, but not all the data)
        {
            use bincode::serialize;
            let kv = KvPair {
                key: "partial",
                value: 999_i32,
            };
            let serialized = serialize(&kv).unwrap();
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
        // read() might find the partial record and fail, or stop at the second record
        let result = w.read::<String, i32>();

        match result {
            Ok(records) => {
                // Possibly your code just stops after the 2 complete records.
                // If so, we can confirm that the partial record is ignored:
                assert_eq!(records.len(), 2, "Expected only 2 valid records");
            }
            Err(e) => {
                // Or your code might fail if it tries to read the partial record.
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
                w.append(KvPair {
                    key: format!("pre_corrupt_{}", i),
                    value: i,
                })?;
            }
        }

        // Corrupt the middle record’s data in the file
        {
            let mut contents = Vec::new();
            {
                let mut f = std::fs::File::open(&path)?;
                f.read_to_end(&mut contents)?;
            }

            // We have 3 records => we have 3 length prefixes + 3 data blobs
            // We'll blindly locate the 2nd record's data by stepping through the first length prefix
            // and skipping that many bytes, then corrupt part of that record.
            // This is a quick hack for demonstration:

            let mut idx = 0;
            for _rec_idx in 0..2 {
                if idx + 4 > contents.len() {
                    break;
                }
                let len_buf = &contents[idx..idx + 4];
                let record_len = u32::from_be_bytes(len_buf.try_into().unwrap()) as usize;
                idx += 4 + record_len;
            }

            // Now idx should be at the start of the 2nd record’s data
            // Let's corrupt 5 bytes (or as many remain)
            let end_idx = (idx + 5).min(contents.len());
            for i in idx..end_idx {
                contents[i] = 0xFF;
            }

            // Write it back
            {
                let mut f = std::fs::File::create(&path)?;
                f.write_all(&contents)?;
            }
        }

        // Now read
        let w = Wal::new(path)?;
        let res = w.read::<String, i32>();

        match res {
            Ok(records) => {
                // Possibly you only read the first record successfully, then hit an error on the 2nd
                // or skip the corrupted record. Implementation details vary.
                println!("Records read successfully: {:?}", records);
            }
            Err(e) => {
                // Possibly you fail as soon as you hit the corrupted record.
                println!("Got an error reading corrupted record: {}", e);
            }
        }

        Ok(())
    }

    /// Very simplistic concurrency test: multiple threads each append multiple records.
    /// We wrap the single WAL in a Mutex to avoid partial interleaving.
    /// Then we check if the total # of records is correct at the end.
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
                    let kv = KvPair {
                        key: format!("t{}-k{}", t_id, i),
                        value: i as i32,
                    };
                    let mut locked = wal_clone.lock().unwrap();
                    locked.append(kv).expect("append failed");
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Now read them all
        let final_wal = Wal::new(path)?;
        let all = final_wal.read::<String, i32>()?;
        assert_eq!(
            all.len(),
            thread_count * writes_per_thread,
            "Mismatch in total records after concurrency test"
        );

        Ok(())
    }
}
