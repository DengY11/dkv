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
   put:    974 ms  (1.02669e+06 ops/sec)
   get:    264 ms  (3.78788e+06 ops/sec)
   update: 638 ms  (1.5674e+06 ops/sec)
   delete: 616 ms  (1.62338e+06 ops/sec)

DKV (batched writes, batch size 5000)
   put:    929 ms  (1.07643e+06 ops/sec)
   get:    337 ms  (2.96736e+06 ops/sec)
   update: 687 ms  (1.4556e+06 ops/sec)
   delete: 637 ms  (1.56986e+06 ops/sec)

DKV (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 7788 ms  (25680.5 ops/sec/thread)
  get (worst thread): 347 ms  (576369 ops/sec/thread)
  total wall time (put+get): 8120 ms  (985222 ops/sec total)

SQLite (txn + prepared statements)
   put:    1430 ms  (699301 ops/sec)
   get:    2832 ms  (353107 ops/sec)
   update: 1996 ms  (501002 ops/sec)
   delete: 1244 ms  (803859 ops/sec)

SQLite (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 10382 ms  (19264.1 ops/sec/thread)
  get (worst thread): 744 ms  (268817 ops/sec/thread)
  total wall time (put+get): 11155 ms  (717167 ops/sec total)
```