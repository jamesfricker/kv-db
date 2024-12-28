# Plan

## TODO

- add some ci (bench/test)

## Done

- I'm pretty sure the skip list implementation is not exactly correct
  - we don't want to duplicate the node across levels, it should be the same node
  - we should just be able to drop down a level, not need to restart each level
- add more benchmarks
- node is key/value
