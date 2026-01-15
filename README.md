# dkv: a LSM-tree key-value store (C++20)

An embedded, high performance LSM-tree key-value store. Ships as a static library plus optional tests, benchmarks,
server, and tools.

## Features
- Write-ahead log for crash safety, memtable backed by sorted std::map
- SSTables with block index + Bloom filter for faster reads
- Level-0 fan-in with leveled compaction to deeper levels; file sizes bounded by `sstable_target_size_bytes`
- Batched writes via `WriteBatch` to amortize WAL fsyncs
- Simple API for `Put`, `Get`, `Delete`, iterator-style `Scan` (with optional snapshot), `Flush`, `Compact`
- Library-first design: `#include <dkv/db.h>` and link against `dkv`

## Build & Run
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### Build Options

This repo can build multiple targets. You can turn them on/off at configure time:

- `DKV_BUILD_SERVER` (default `ON`): build `dkv-server`
- `DKV_BUILD_UTILS` (default `ON`): build `dkv-dump`
- `DKV_BUILD_TESTS` (default `ON`): build `dkv_tests`
- `DKV_BUILD_BENCHMARKS` (default `ON`): build benchmarks
- `DKV_BUILD_EXAMPLES` (default `ON`): build `dkv_example`

Example: build only the `dkv` library:

```bash
cmake -S . -B build -DDKV_BUILD_SERVER=OFF -DDKV_BUILD_UTILS=OFF -DDKV_BUILD_TESTS=OFF -DDKV_BUILD_BENCHMARKS=OFF -DDKV_BUILD_EXAMPLES=OFF
cmake --build build -j
```

## dkv-server (Redis protocol)

`dkv-server` is a lightweight Redis-protocol (RESP2) server backed by the embedded `dkv` library.

### Start the server

```bash
./build/dkv-server \
  --data-dir ./dkv-data \
  --bind 127.0.0.1 \
  --port 6379 \
  --subreactors 4 \
  --workers 8
```

Notes:

- `--data-dir` stores all persistent files (WAL, manifest, SSTables).
- `--subreactors 0` / `--workers 0` means “auto”.
- Stop with `Ctrl+C` (SIGINT) or `kill` (SIGTERM).

### Connect to the server

Use any Redis client. Quick sanity check with `redis-cli`:

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 SET k v
redis-cli -h 127.0.0.1 -p 6379 GET k
redis-cli -h 127.0.0.1 -p 6379 DEL k
```

Or open an interactive session:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

For supported commands and configuration flags, see `server/README.md`.

## dkv-dump (WAL/SST debug tool)

`dkv-dump` dumps WAL and SSTable contents for debugging.

### Dump WAL

Dump a single WAL file:

```bash
./build/dkv-dump wal ./dkv-data/wal.log
```

Dump a directory (will read `wal.log` and `wal-*.log` in order):

```bash
./build/dkv-dump wal ./dkv-data --limit 50
```

### Dump SSTables

Dump one SST file:

```bash
./build/dkv-dump sst ./dkv-data/sst/000001.sst
```

Dump a directory (will scan and dump all `*.sst` under it):

```bash
./build/dkv-dump sst ./dkv-data/sst --verify-crc --limit 20
```

### Common flags

- `--out <file>`: write output to file (use `-` for stdout)
- WAL: `--ignore-crc`, `--limit N`
- SST: `--verify-crc`, `--blocks`, `--limit N`

Run `./build/dkv-dump --help` for the full usage text.

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

See docs/ARCHITECTURE.md for a detailed walkthrough of the design, data structures, and how memtables/SSTables differ from LevelDB. See docs/OPTIONS.md for a full option matrix and their performance trade-offs.

## Benchmarks
see docs/BENCHMARKS.md

## Optional compression backend
see docs/COMPRESSION.md

## License
MIT License (see LICENSE)
