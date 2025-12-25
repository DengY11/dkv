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
   put:    929 ms  (1.07643e+06 ops/sec)
   get:    261 ms  (3.83142e+06 ops/sec)
   update: 613 ms  (1.63132e+06 ops/sec)
   delete: 553 ms  (1.80832e+06 ops/sec)

DKV (batched writes, batch size 5000)
   put:    893 ms  (1.11982e+06 ops/sec)
   get:    308 ms  (3.24675e+06 ops/sec)
   update: 644 ms  (1.5528e+06 ops/sec)
   delete: 583 ms  (1.71527e+06 ops/sec)

DKV (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 7670 ms  (26075.6 ops/sec/thread)
  get (worst thread): 370 ms  (540541 ops/sec/thread)
  total wall time (put+get): 8003 ms  (999625 ops/sec total)

SQLite (txn + prepared statements)
   put:    1442 ms  (693481 ops/sec)
   get:    2700 ms  (370370 ops/sec)
   update: 2016 ms  (496032 ops/sec)
   delete: 1217 ms  (821693 ops/sec)

SQLite (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 10422 ms  (19190.2 ops/sec/thread)
  get (worst thread): 679 ms  (294551 ops/sec/thread)
  total wall time (put+get): 11143 ms  (717940 ops/sec total)

LevelDB (batched writes, batch size 5000)
   put:    870 ms  (1.14943e+06 ops/sec)
   get:    1270 ms  (787402 ops/sec)
   update: 808 ms  (1.23762e+06 ops/sec)
   delete: 1438 ms  (695410 ops/sec)

LevelDB (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 26436 ms  (7565.44 ops/sec/thread)
  get (worst thread): 5527 ms  (36186 ops/sec/thread)
  total wall time (put+get): 31972 ms  (250219 ops/sec total)
```