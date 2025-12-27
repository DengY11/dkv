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

## Benchmarks
详见 `docs/BENCHMARKS.md`，包含 `dkv_bench_suite` 的运行方式、参数说明，以及 DKV/SQLite/LevelDB 的示例结果。
