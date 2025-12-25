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
Benchmarking 100000000 ops
DKV (single ops)
   put:    127164 ms  (786386 ops/sec)
   get:    50640 ms  (1.97472e+06 ops/sec)
   update: 134503 ms  (743478 ops/sec)
   delete: 132494 ms  (754751 ops/sec)

DKV (batched writes, batch size 500000)
   put:    131865 ms  (758351 ops/sec)
   get:    61137 ms  (1.63567e+06 ops/sec)
   update: 144900 ms  (690131 ops/sec)
   delete: 143276 ms  (697954 ops/sec)

DKV (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 7863 ms  (25435.6 ops/sec/thread)
  get (worst thread): 362 ms  (552486 ops/sec/thread)
  total wall time (put+get): 8201 ms  (975491 ops/sec total)

SQLite (txn + prepared statements)
   put:    165597 ms  (603876 ops/sec)
   get:    304237 ms  (328691 ops/sec)
   update: 271286 ms  (368615 ops/sec)
   delete: 162341 ms  (615987 ops/sec)

SQLite (multithreaded, 20 threads, 200000 ops each)
  put (worst thread): 10491 ms  (19064 ops/sec/thread)
  get (worst thread): 744 ms  (268817 ops/sec/thread)
  total wall time (put+get): 11238 ms  (711870 ops/sec total)
```