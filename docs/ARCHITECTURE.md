# Architecture

`dkv` is a compact LSM store: WAL + CRC for durability, MANIFEST for recovery, active/immutable memtables with background flush threads, a serialized apply stage, background compaction threads, and basic metrics for observability. The code stays small so components are easy to swap or extend.

## Components and Data Structures
- **Memtable (`src/memtable.*`)**: sharded `pmr::unordered_map` (16 shards by default) with per-shard locks and a monotonic buffer resource to reduce allocations. Snapshot sorts each shard locally, then does a k-way merge; flush uses `string_view` views to avoid extra copies. Tracks approximate memory to trigger flushes. One active memtable plus a queue of immutables flushed in the background.
- **WAL (`src/wal.*`)**: append-only log with per-record CRC32. Optional per-write fsync, periodic background sync, and parent-dir fsync on reset/rotation. Replay stops at the first corrupted record. Rotates to `wal-<max_seq>.log` when sealing a memtable.
- **SSTable (`src/sstable.*`)**: immutable sorted runs. Entry encoding: `[deleted byte][seq][klen][vlen][key][value]`. Blocked layout with sparse block index and Bloom filter; footer stores offsets and magic. Optional block cache (`block_cache_capacity_bytes`). Bloom filters are lazy-loaded on demand; an optional LRU (`bloom_cache_capacity_bytes`) holds shared copies and can pin upper-level blooms in memory. Data blocks optionally compress at write time; the backend is chosen at build time, and each block can fall back to raw if compression is ineffective.
- **Manifest (`data_dir/MANIFEST`)**: authoritative list of live SSTables and levels. Written atomically (temp file → fsync file → fsync parent dir → rename). Startup prefers manifest, falls back to a directory scan. Recovery replays `wal-*.log` plus `wal.log` in order.
- **DB (`src/db.*`)**: coordinates WAL, memtables, SSTables. When the active memtable exceeds `memtable_soft_limit_bytes`, it is swapped into the immutable queue, WAL is rotated, a new active memtable is created. Multiple flush threads write immutables to L0 SSTs. A single apply thread serializes level updates and MANIFEST rewrite, deletes WAL segments, and then enqueues compaction work. Background compaction threads drain the compaction queue and rewrite lower levels. Leveled compaction keeps L1+ non-overlapping.
- **Metrics**: cumulative counts for puts/deletes/gets/batches, flush/compaction counts/durations/bytes, and WAL syncs (`DB::GetMetrics`).

## File Layout
- `<data_dir>/wal.log` – WAL
- `<data_dir>/MANIFEST` – manifest of live SSTables/levels
- `<data_dir>/sst/sst-l<level>-*.sst` – SSTables grouped by level (L0 may overlap; L1+ sorted, non-overlapping)

## Write Path
1. Assign a monotonically increasing sequence.
2. Append to WAL (CRC protected). If `sync_wal` or `WriteOptions::sync`, fsync immediately (or rely on background WAL sync interval).
3. Apply to the active memtable (sharded hash table).
4. If the memtable exceeds `memtable_soft_limit_bytes`, rotate WAL to `wal-<max_seq>.log`, move the active memtable into the immutable queue, create a fresh active memtable. Multiple flush threads consume immutables, write L0 SSTs, and enqueue flush results.
5. A single apply thread registers new L0 tables (holds `sstable_mu_`), rewrites MANIFEST (holds `manifest_mu_`), deletes the flushed WAL segment, updates metrics, and schedules compaction.
6. Background compaction threads drain the compaction queue, run `CompactLevel`, rewrite MANIFEST, and update metrics.

`WriteBatch` groups multiple ops under one WAL sync, reducing fsync overhead while remaining atomic at the DB level.

Background WAL sync: if `wal_sync_interval_ms > 0` and `sync_wal == false`, a thread calls `wal_->Sync(false)` periodically; `WriteOptions::sync` still forces fsync.

## Read Path
1. Active memtable.
2. Immutable memtables (newer → older).
3. L0 SSTables newest-first (may overlap).
4. L1+ using key ranges + Bloom to prune (non-overlapping per level).
5. `Scan` merges the latest version per key across memtables and SSTables (in-memory merge for simplicity).

## Compaction
- Triggered when L0 file count exceeds `level0_file_limit`, or when `LevelBytes(level) > LevelMaxBytes(level)` for L1+ (`level_base_bytes * level_size_multiplier^(level-1)`). Scheduling is done by apply/flush completion; execution is on background compaction threads.
- Inputs: all of level `L` plus overlapping `L+1`.
- Process: load entries, sort by key asc / seq desc, keep newest per key (and optionally keep a snapshot-visible older version), chunk by `sstable_target_size_bytes` into `L+1`.
- Cleanup: delete old SSTs, insert new ones, rewrite manifest (serialized by `manifest_mu_`), update metrics (count/duration/input+output bytes).

## Durability and Integrity
- WAL records carry CRC32; replay verifies and stops at the first bad record.
- Manifest is fsynced (file + parent dir) to persist the set of live SSTables.
- WAL segments are rotated and deleted after successful flush; rotation fsyncs parent dir to persist directory entries.
- SSTable footer carries magic/offsets; Bloom/index reads are bounds-checked.

## Metrics (DB::GetMetrics)
- Ops: `puts`, `deletes`, `gets`, `batches`
- Flush: `flushes`, `flush_ms`, `flush_bytes`
- Compaction: `compactions`, `compaction_ms`, `compaction_input_bytes`, `compaction_output_bytes`
- WAL: `wal_syncs` (per-write syncs + background syncs)

## Compared to LevelDB
- **Memtable**: LevelDB uses a skiplist; DKV uses a sharded hash table with per-shard locks plus a sort-and-merge snapshot at flush time. This reduces write contention for many writers but adds a sort during flush. Both support batched writes.
- **SSTable blocks**: LevelDB uses restart points for prefix compression inside blocks. DKV stores raw entries (or fully compressed per block with Snappy/Zstd/LZ4 chosen at build time). Index entries include block size (header+payload) and header carries raw/stored sizes and compression code.
- **Caches/Bloom**: Both use block cache and Bloom filters; DKV also supports a separate Bloom cache that can pin upper-level blooms.
- **Compaction**: Same leveled model (L0 overlap, L1+ non-overlap). DKV’s compaction is simpler (no partial overlap trimming) and rewrites the manifest after each compaction.
- **Snapshots**: LevelDB exposes long-lived snapshots that prevent compaction from dropping versions newer than the snapshot sequence. DKV’s `ReadOptions::snapshot`/`snapshot_seq` build a static view at iterator creation time and, while the iterator lives, compaction retains the latest version per key that is ≤ the oldest active snapshot seq. Once all snapshots are released, older versions may be dropped on compaction. There is no explicit snapshot handle API beyond iterator lifetime.
- **Durability knobs**: Both offer per-write sync; DKV adds optional periodic WAL sync (`wal_sync_interval_ms`).

## Example
See `examples/example.cc` for a minimal usage sample: open DB, put/get, batch write, scan, and print metrics.
