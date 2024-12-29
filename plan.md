# Plan

## TODO

- db can start/stop and reload data from previous runs
- store immutable data using sstables
- improve sstables with lsm trees and leveled compaction
- bloom filter to improve read performance

## Done

- create a WAL
- add some ci (bench/test)
- I'm pretty sure the skip list implementation is not exactly correct
  - we don't want to duplicate the node across levels, it should be the same node
  - we should just be able to drop down a level, not need to restart each level
- add more benchmarks
- node is key/value

## Notes

### WAL

- we need to define a record type to be what we will write to the file
  - index, len(key), key, len(value), value
  - we need to write every insert to disk, and then read the file on restart (persistence)
  - probably want some kind of class do to this, similar to the skip list
  - will need some overarching concept of the db, so we can use both the wal and the skip list
    - and then also add compaction etc later
