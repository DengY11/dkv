# Architecture

`dkv` is a compact LSM store: WAL + MANIFEST for recovery, an active memtable plus immutable memtables, background flush/apply/compaction threads, and basic metrics for observability. The design keeps components small and easy to swap.

## Components and Data Structures
- **Memtable (`src/memtable.*`)**: skiplist keyed by `(key asc, seq desc)` with arena-backed storage. Writes are protected by a shared mutex; reads take a shared lock. Entries store `string_view` into the arena. The DB keeps one active memtable and a queue of immutable memtables for flushing.
- **WAL (`src/wal.*`)**: append-only log with per-record CRC32. Optional per-write fsync, optional background sync, and WAL rotation when sealing a memtable.
- **SSTable (`src/sstable.*`)**: immutable sorted runs. Blocked layout with sparse block index and Bloom filter; footer stores offsets and magic. Optional data-block compression at write time. Caches:
  - block cache (decoded entries)
  - raw block cache (compressed/raw bytes)
  - bloom cache (lazy-loaded blooms; upper levels may be pinned)
- **Manifest (`data_dir/MANIFEST`)**: authoritative list of live SSTables/levels. Written atomically (tmp + fsync + rename). Startup prefers MANIFEST, falls back to directory scan.
- **DB (`src/db.*`)**: coordinates WAL, memtables, SSTables, and background threads. When the active memtable exceeds `memtable_soft_limit_bytes`, it is swapped out and queued as immutable; WAL is rotated; a new memtable becomes active.
- **Metrics**: cumulative counts for puts/deletes/gets/batches, flush/compaction counts/durations/bytes, WAL syncs, and cache stats (`DB::GetMetrics`).

## File Layout
- `<data_dir>/wal.log` – active WAL
- `<data_dir>/wal-<max_seq>.log` – rotated WAL segments
- `<data_dir>/MANIFEST` – manifest of live SSTables/levels
- `<data_dir>/sst/sst-l<level>-<id>.sst` – SSTables grouped by level

## Write Path
1. Allocate a sequence number.
2. Append to WAL (CRC protected). If `sync_wal` or `WriteOptions::sync`, fsync immediately; otherwise optional periodic sync.
3. Apply to active memtable.
4. If memtable exceeds `memtable_soft_limit_bytes`, rotate WAL and move the current memtable into the immutable queue; install a new active memtable.
5. Flush threads write immutables into L0 SSTables.
6. A single apply thread registers new L0 tables, rewrites MANIFEST, deletes the flushed WAL segment, updates metrics, and schedules compaction.

`WriteBatch` groups multiple ops under one WAL sync, reducing fsync overhead while remaining atomic at the DB level.

## Read Path
1. Active memtable.
2. Immutable memtables (newer → older).
3. L0 SSTables newest-first (may overlap).
4. L1+ SSTables using key ranges + Bloom (non-overlapping per level).
5. `Scan` builds an in-memory merged view across memtables and SSTables for simplicity.

## Compaction
- Triggered when L0 file count exceeds `level0_file_limit`, or when `LevelBytes(level) > LevelMaxBytes(level)` for L1+ (`level_base_bytes * level_size_multiplier^(level-1)`).
- Inputs: all of level `L` plus overlapping `L+1` (L0 compaction includes all L0).
- Process: merge entries by key; keep the newest version and optionally keep a snapshot-visible version (≤ oldest active snapshot).
- Outputs: new L+1 SSTables chunked by `sstable_target_size_bytes`.
- Cleanup: delete old SSTs, insert new ones, rewrite MANIFEST, update metrics.

## Snapshots
`ReadOptions::snapshot`/`snapshot_seq` create a static view for iterators. While snapshots are active, compaction retains the newest version per key that is ≤ the oldest active snapshot sequence.

## Durability and Integrity
- WAL records carry CRC32; replay verifies and stops at the first bad record.
- MANIFEST is fsynced (file + parent dir) to persist the set of live SSTables.
- WAL segments are rotated and deleted after successful flush.
- SSTable footer carries magic/offsets; Bloom/index reads are bounds-checked.

## Example
See `examples/example.cc` and `examples/resp_client_ops.cpp` for embedded and RESP client usage.
