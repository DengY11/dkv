# dkv: a tiny LSM-tree key-value store (C++20)

An embedded, LevelDB-like key-value store implemented from scratch with a clear layout, minimal abstractions, and performance-friendly defaults. Ships as a small static library plus tests and benchmarks.

## Features
- Write-ahead log for crash safety, memtable backed by sorted std::map
- SSTables with block index + Bloom filter for faster reads
- Level-0 fan-in with leveled compaction to deeper levels; file sizes bounded by `sstable_target_size_bytes`
- Batched writes via `WriteBatch` to amortize WAL fsyncs
- Simple API for `Put`, `Get`, `Delete`, `Scan`, `Flush`, `Compact`
- Library-first design: `#include <dkv/db.h>` and link against `dkv`

## Build & Run
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build         # run unit tests
./build/dkv_bench              # run CRUD + SQLite comparison benchmark
# SQLite comparison builds automatically if system SQLite3 dev libs are available
```

## Layout
- `include/` public headers
- `src/` storage, WAL, memtable, SSTable, DB plumbing
- `tests/` self-contained assertions without third-party deps
- `benchmarks/` micro-benchmark for put/get throughput
- `docs/` architecture notes and usage details

## Quickstart
```cpp
#include "dkv/db.h"

dkv::Options opts;
opts.data_dir = "my-data";
std::unique_ptr<dkv::DB> db;
dkv::DB::Open(opts, db);
db->Put({}, "hello", "world");
std::string val;
db->Get({}, "hello", val);

dkv::WriteBatch batch;
batch.Put("a", "1");
batch.Put("b", "2");
batch.Delete("a");
db->Write({}, batch);  // single WAL sync for the entire batch
```

See `docs/` for design notes and tuning tips.

## Tuning (key options)
- `memtable_soft_limit_bytes`: flush trigger
- `sstable_target_size_bytes`: output file size hint; compaction splits output accordingly
- `level0_file_limit`, `level_base_bytes`, `level_size_multiplier`: leveled compaction thresholds
- `sstable_block_size_bytes`, `bloom_bits_per_key`: read-path trade-offs (Bloom and block index)
- `block_cache_capacity_bytes`: enable LRU caching of SSTable blocks to cut repeated disk reads

## Benchmark (DKV vs Sqlite)
```bash
Benchmarking 1000000 ops

DKV (single ops)
   put:    1108 ms  (902527 ops/sec)
   get:    400 ms  (2.5e+06 ops/sec)
   update: 897 ms  (1.11483e+06 ops/sec)
   delete: 766 ms  (1.30548e+06 ops/sec)

DKV (batched writes, batch size 5000)
   put:    684 ms  (1.46199e+06 ops/sec)
   get:    377 ms  (2.65252e+06 ops/sec)
   update: 748 ms  (1.3369e+06 ops/sec)
   delete: 683 ms  (1.46413e+06 ops/sec)

DKV (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 7013 ms  (28518.5 ops/sec/thread)
  get (worst thread): 309 ms  (647249 ops/sec/thread)
  total wall time (put+get): 7301 ms  (1.09574e+06 ops/sec total)

SQLite (txn + prepared statements)
   put:    1479 ms  (676133 ops/sec)
   get:    2789 ms  (358551 ops/sec)
   update: 2014 ms  (496524 ops/sec)
   delete: 1197 ms  (835422 ops/sec)

SQLite (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 10392 ms  (19245.6 ops/sec/thread)
  get (worst thread): 854 ms  (234192 ops/sec/thread)
  total wall time (put+get): 11011 ms  (726546 ops/sec total)

LevelDB (batched writes, batch size 5000)
   put:    658 ms  (1.51976e+06 ops/sec)
   get:    624 ms  (1.60256e+06 ops/sec)
   update: 793 ms  (1.26103e+06 ops/sec)
   delete: 768 ms  (1.30208e+06 ops/sec)

LevelDB (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 34185 ms  (5850.52 ops/sec/thread)
  get (worst thread): 4538 ms  (44072.3 ops/sec/thread)
  total wall time (put+get): 38688 ms  (206782 ops/sec total)
```