# Options and Performance Notes

DKV exposes tuning knobs through `dkv::Options` (compile-time compression backend is set via `DKV_COMPRESSION` in CMake; runtime just toggles `enable_compress`).

- `memtable_soft_limit_bytes` – Larger → fewer flushes/L0 files; smaller → more frequent flush/compaction and smoother write latency.
- `sync_wal` / `WriteOptions::sync` – `true` fsyncs every write (strong durability, higher latency). `false` relies on OS buffering; combine with `wal_sync_interval_ms` for periodic sync.
- `wal_sync_interval_ms` – Background WAL fsync cadence. Non-zero lowers per-op latency but risks bounded data loss up to the interval.
- `flush_thread_count` – Background flush threads for immutable memtables. Too many can contend on disk.
- `max_immutable_memtables` – Queue depth of immutables before writers block. Increase to absorb bursts; decrease to bound memory/stall sooner.
- `enable_crc` – Disable only for perf experiments; CRC protects WAL replay.
- `compaction_thread_count` – Background compaction threads. More threads reduce L0 pile-up but add write amp/IO contention.
- `sstable_target_size_bytes` – Larger files reduce index/Bloom overhead; too large increases compaction cost.
- `sstable_block_size_bytes` – Larger blocks reduce index size; smaller blocks improve point lookups.
- `bloom_bits_per_key` – Higher reduces false positives at extra space cost.
- `block_cache_capacity_bytes` – LRU cache for decoded data blocks; improves hot-read latency. Set to 0 to disable.
- `raw_block_cache_capacity_bytes` – LRU cache for raw/compressed blocks; reduces decompression and IO. Set to 0 to disable.
- `bloom_cache_capacity_bytes` – Cache for Bloom filters to reduce bloom IO (upper levels may be pinned internally).
- `level0_file_limit`, `level_base_bytes`, `level_size_multiplier`, `max_levels` – Control compaction pressure. Lower thresholds compact sooner (lower read amp) but increase write amp/CPU.
- `data_dir` – Filesystem placement; choose faster storage for better latency.
- `enable_compress` – Attempts block compression if the binary was built with a backend. If no backend was built, this flag has no effect.
- `ReadOptions::snapshot` / `ReadOptions::snapshot_seq` – Iterator builds a static view at the current max seq (or the explicit seq). While snapshots are active, compaction retains the newest version per key that is ≤ the oldest active snapshot seq.

Compile-time:
- `-DDKV_COMPRESSION=auto|snappy|zstd|lz4|none` – Chooses the compiled-in backend; see `docs/COMPRESSION.md`.
