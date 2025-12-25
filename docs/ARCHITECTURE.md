# Architecture and Tuning

`dkv` is a minimal LSM-tree store with durability (WAL + fsync), CRC protection, a manifest for crash recovery, dual memtables with background flush, and basic metrics for observability. Code is intentionally compact to make it easy to extend.

## Components
- **Memtable (`src/memtable.*`)**: in-memory sorted map on `std::pmr::map` + arena to reduce allocations. Tracks approximate memory footprint to trigger flush. Supports `Put/Delete/Get/Snapshot`. Two memtables are in flight: an active one for writes and a queue of immutables flushed by a background thread.
- **WAL (`src/wal.*`)**: append-only log with per-record CRC32. Optional per-write fsync, background periodic sync, and parent-dir fsync on reset/rotation. Replay stops at the first corrupted record to avoid applying garbage. WAL rotates to `wal-<max_seq>.log` when memtable is sealed.
- **SSTable (`src/sstable.*`)**: immutable sorted run. Entries encoded as `[deleted byte][seq][klen][vlen][key][value]`. Data is block-organized with sparse block index and a Bloom filter; footer stores offsets and magic. Block cache is optional (`block_cache_capacity_bytes`).
- **Manifest (`data_dir/MANIFEST`)**: records the active SSTables and their levels. Written atomically via temp file + fsync file + fsync parent dir + rename. Startup prefers manifest; falls back to directory scan if absent. Recovery replays all `wal-*.log` segments plus `wal.log` in sequence order.
- **DB (`src/db.*`)**: orchestrates WAL, memtables, SSTables. When the active memtable exceeds `memtable_soft_limit_bytes`, it is swapped into the immutable queue, WAL is rotated, and a background thread flushes immutables to L0 SSTables and deletes their WAL segments. Leveled compaction merges L0 into L1+ maintaining non-overlapping key ranges for L1+.
- **Metrics**: cumulative counters for puts/deletes/gets/batches, flush/compaction counts, durations, bytes, and WAL syncs (`DB::GetMetrics`).

## File Layout
- `<data_dir>/wal.log` – WAL
- `<data_dir>/MANIFEST` – manifest of live SSTables/levels
- `<data_dir>/sst/sst-l<level>-*.sst` – SSTables grouped by level (L0 may overlap; L1+ sorted, non-overlapping)

## Write Path
1. Assign monotonically increasing sequence.
2. Append to WAL (CRC protected). If `sync_wal` or `WriteOptions::sync`, fsync immediately.
3. Apply to active memtable (pmr map).
4. If memtable exceeds `memtable_soft_limit_bytes`, rotate WAL to `wal-<max_seq>.log`, move the active memtable into the immutable queue, create a fresh active memtable, let the background thread flush immutables to L0 and delete their WAL segments, and rewrite manifest.

`WriteBatch` groups multiple ops under one WAL sync, reducing fsync overhead while remaining atomic at the DB level.

Background WAL sync: if `wal_sync_interval_ms > 0` and `sync_wal==false`, a thread calls `wal_->Sync(false)` periodically; manual sync via `WriteOptions::sync` still forces fsync.

## Read Path
1. Check active memtable.
2. Check immutable memtables in the flush queue (newer to older).
3. Check L0 SSTables newest-first (may overlap).
4. Check L1+ using key ranges + Bloom to prune (non-overlapping per level).
5. `Scan` merges the latest version per key across memtables and SSTables (in-memory merge for simplicity).

## Compaction
- Triggered when L0 file count exceeds `level0_file_limit`, or when `LevelBytes(level) > LevelMaxBytes(level)` for L1+ (`level_base_bytes * level_size_multiplier^(level-1)`).
- Inputs: all of level `L` plus overlapping `L+1`.
- Process: load all entries, sort by key asc / seq desc, keep newest per key, chunk by `sstable_target_size_bytes` into `L+1`.
- Cleanup: delete old SSTs, insert new ones, rewrite manifest, update metrics (count/duration/input+output bytes).

## Durability and Integrity
- WAL records carry CRC32; replay verifies and stops at the first bad record.
- Manifest is fsynced (file + parent dir) to persist the set of live SSTables.
- WAL segments are rotated and deleted after successful flush; rotation fsyncs parent dir to persist directory entries.
- SSTable footer carries magic/offsets; Bloom/index are read bounds-checked.

## Metrics (DB::GetMetrics)
- Ops: `puts`, `deletes`, `gets`, `batches`
- Flush: `flushes`, `flush_ms`, `flush_bytes`
- Compaction: `compactions`, `compaction_ms`, `compaction_input_bytes`, `compaction_output_bytes`
- WAL: `wal_syncs` (per-write syncs + background syncs)

## Key Parameters and Tuning
- **Durability vs latency**
  - `Options::sync_wal` / `WriteOptions::sync`: fsync every write vs app-level control.
  - `wal_sync_interval_ms`: periodic background sync (set to >0 for durability with lower per-op latency).
- **Memory/flush cadence**
  - `memtable_soft_limit_bytes`: larger -> fewer flushes, larger L0 runs; smaller -> quicker durability and compaction pressure.
- **SST layout**
  - `sstable_target_size_bytes`: output run size; larger reduces file count but increases compaction chunk size.
  - `sstable_block_size_bytes`: read amplification vs index/bloom overhead.
  - `bloom_bits_per_key`: higher reduces false positives at the cost of space.
- **Levels/compaction pressure**
  - `level0_file_limit`: when L0 compacts.
  - `level_base_bytes`, `level_size_multiplier`: total bytes allowed per level; smaller values compact more aggressively.
  - `block_cache_capacity_bytes`: enable to reduce read IO for hot blocks.
- **Concurrency**
  - Current writes are serialized by `mu_`; reads use shared lock for SST structure. Multi-thread writes share one DB; use `WriteBatch` to amortize WAL sync if needed.

## Example
See `examples/example.cc` for a minimal usage sample: open DB, put/get, batch write, scan, and print metrics.
