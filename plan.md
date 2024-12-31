# Plan

## TODO

### Core

- store immutable data using sstables to provide complete persistence
- create index for sstables to improve reads
- do levelled compaction of sstables
- bloom filter to improve read performance

### Improvements

- make WAL/Memtable writes atomic?
  - what happens if we write to the WAL but not to the memtable?
- make the types for the db easier to use

## Done

- fix the types of the skip list - use `Vec<u8>` for both keys and values (bytes)?
- db can start/stop and reload data from previous runs
- create a WAL
- add some ci (bench/test)
- I'm pretty sure the skip list implementation is not exactly correct
  - we don't want to duplicate the node across levels, it should be the same node
  - we should just be able to drop down a level, not need to restart each level
- add more benchmarks
- node is key/value

## Notes

### SSTables

- we want to implement a 'flush' method on the Database(?)
- this will create an `ImmutableSSTable` (TODO) object, and write the contents of the level 0 skip list to a file
  - need to create some kind of threshold to know when to flush
- On startup, we need to reload all the `ImmutableSSTable` objects, each having a reference to it's file
- when we do a get, we need to search the current memtable (skip list) and also through the SSTables
  - we search in reverse order, and return the first value that we find

### WAL

- we need to define a record type to be what we will write to the file
  - index, len(key), key, len(value), value
  - we need to write every insert to disk, and then read the file on restart (persistence)
  - probably want some kind of class do to this, similar to the skip list
  - will need some overarching concept of the db, so we can use both the wal and the skip list
    - and then also add compaction etc later
