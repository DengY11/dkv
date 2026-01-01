# Options and Performance Notes

DKV exposes tuning knobs through `dkv::Options` (compile-time compression backend is set via `DKV_COMPRESSION` in CMake; runtime just toggles `enable_compress`).

- `memtable_soft_limit_bytes` – Larger → fewer flushes and fewer L0 files but bigger write stalls when flushing; smaller → more frequent flush/compaction and better latency stability.
- `memtable_shard_count` – More shards reduce write lock contention; too many shards increase flush sort/merge cost.
- `sync_wal` / `WriteOptions::sync` – `true` fsyncs every write (strong durability, higher latency). `false` relies on OS buffering; combine with `wal_sync_interval_ms` for periodic sync.
- `wal_sync_interval_ms` – Background WAL fsync cadence. Non-zero lowers per-op latency but risks bounded data loss up to the interval.
- `flush_thread_count` – Number of background threads flushing immutable memtables to L0 SSTables. Larger can increase throughput if flush is CPU-bound; too large can contend on disk.
- `max_immutable_memtables` – Queue depth of immutables before writers block. Increase to let background flush absorb bursts; decrease to bound memory/stall sooner.
- `enable_crc` – Disable only for perf experiments; CRC protects WAL replay.
- `compaction_thread_count` – Number of background compaction threads. More threads can reduce L0 pile-up/read amp at the cost of extra write amp and IO contention.
- `sstable_target_size_bytes` – Larger files reduce index/Bloom overhead and compaction fan-out; too large raises compaction pause and write amplification when rewriting big runs.
- `sstable_block_size_bytes` – Larger blocks reduce index size and Bloom checks; smaller blocks improve point-lookups and reduce read amplification for sparse reads.
- `bloom_bits_per_key` – Higher reduces false positives (fewer block reads) at extra space cost; low values can amplify read IO.
- `block_cache_capacity_bytes` – Enables LRU cache for data blocks; improves hot-read latency and reduces disk IO. Set to 0 to disable.
- `bloom_cache_capacity_bytes` / `pin_bloom` – Cache/pin Bloom filters to cut bloom reads from disk, especially on upper levels.
- `level0_file_limit`, `level_base_bytes`, `level_size_multiplier`, `max_levels` – Control compaction pressure. Lower thresholds compact sooner (lower read amp) but increase write amp and CPU. Higher thresholds defer compaction (cheaper writes) but raise read amp and L0 overlap.
- `data_dir` – Filesystem placement; choose faster storage for better latency.
- `enable_compress` – Attempts block compression if the binary was built with a backend. Saves space and IO if data is compressible; otherwise falls back to raw blocks per-block. If built with `-DDKV_COMPRESSION=none` or no backend found, this flag has no effect.
- `ReadOptions::snapshot` / `ReadOptions::snapshot_seq` – If `snapshot=true`, iterator `Scan` builds a static view at the current max sequence; `snapshot_seq` overrides with an explicit sequence upper bound. While snapshots are active, compaction retains the latest version per key that is ≤ the oldest active snapshot seq; once all snapshots are gone, old versions can be dropped.

Compile-time:
- `-DDKV_COMPRESSION=auto|snappy|zstd|lz4|none` – Chooses the compiled-in backend; see `docs/COMPRESSION.md`.
