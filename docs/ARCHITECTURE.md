# Architecture

`dkv` is a minimal LSM-tree store designed to be easy to read and extend.

## Components
- **Memtable (`src/memtable.*`)**: in-memory sorted map that holds the latest updates. Tracks approximate memory footprint to trigger flushes.
- **WAL (`src/wal.*`)**: append-only write-ahead log. Every mutation is logged before touching memory; optional fsync per write or batched.
- **SSTable (`src/sstable.*`)**: immutable sorted run on disk. Data is grouped into blocks with a sparse block index and a per-file Bloom filter. Footer stores offsets for Bloom/index plus sequence metadata.
- **DB (`src/db.*`)**: orchestrates WAL, memtable, SSTables. Flush writes the memtable to a new Level-0 SSTable and truncates the WAL. Leveled compaction merges Level-0 into Level-1 and beyond, splitting output by `sstable_target_size_bytes` and keeping Level-1+ ranges非重叠。可选块缓存（`block_cache_capacity_bytes`）减少重复读。

## File Layout
- `<data_dir>/wal.log` – WAL
- `<data_dir>/sst/sst-l<level>-*.sst` – SSTables grouped by level (Level-0 may overlap, Level-1+ kept sorted/non重叠)

## Write Path
1. Assign monotonically increasing sequence.
2. Append to WAL (optionally fsync).
3. Apply to memtable.
4. If memtable exceeds `memtable_soft_limit_bytes`, flush to a new Level-0 SSTable.

`WriteBatch` groups multiple `Put`/`Delete` operations under one WAL sync, reducing fsync overhead while remaining atomic at the DB level.

## Read Path
1. Check memtable.
2. Check Level-0 SSTables (newest first, may overlap); then Level-1+ (non重叠) using key ranges + Bloom to prune.
3. `Scan` merges the latest version per key across memtable and SSTables (in-memory merge for simplicity).

## Compaction
Leveled compaction:
- Level-0 is bounded by `level0_file_limit`; when exceeded, compact all L0 files plus overlapping Level-1 files into Level-1 outputs split by `sstable_target_size_bytes`.
- Each deeper level is size-bound by `level_base_bytes * level_size_multiplier^(level-1)`; when exceeded, compact that level into the next.
- Outputs maintain Level-1+ non重叠 key ranges to reduce read放大.

## Tuning
- `memtable_soft_limit_bytes`: increase for fewer flushes/larger runs, decrease for quicker durability.
- `sync_wal` / `WriteOptions::sync`: trade latency for durability.
- `sstable_target_size_bytes`: controls SSTable split size during flush/compaction.
- `sstable_block_size_bytes`, `bloom_bits_per_key`: read路径性能 vs. 内存/文件大小.
- `level0_file_limit`, `level_base_bytes`, `level_size_multiplier`: control compaction pressure and read放大 across levels.
