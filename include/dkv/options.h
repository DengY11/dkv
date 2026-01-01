#pragma once

#include <cstddef>
#include <filesystem>

namespace dkv {

struct Options {
  std::filesystem::path data_dir{"dkv-data"};
  // Memtable bytes before triggering a flush.
  std::size_t memtable_soft_limit_bytes{16 * 1024 * 1024};
  // Force WAL fsync on every write.
  bool sync_wal{false};
  // Target SSTable file size before rolling to a new one (rough hint).
  std::size_t sstable_target_size_bytes{8 * 1024 * 1024};
  // SSTable block size for data blocks.
  std::size_t sstable_block_size_bytes{4 * 1024};
  // Bloom filter bits per key.
  std::size_t bloom_bits_per_key{10};
  // Bloom cache size in bytes (0 disables bloom cache; bloom then loads lazily per use).
  std::size_t bloom_cache_capacity_bytes{0};
  // Block cache size in bytes (0 disables cache).
  std::size_t block_cache_capacity_bytes{32 * 1024 * 1024};
  // Max Level-0 file count before compaction.
  std::size_t level0_file_limit{6};
  // Base bytes for Level-1; each deeper level grows by multiplier.
  std::size_t level_base_bytes{128 * 1024 * 1024};
  std::size_t level_size_multiplier{10};
  std::size_t max_levels{4};
  // Number of background flush threads for memtables.
  std::size_t flush_thread_count{1};
  // Max immutable memtables queued before blocking writers.
  std::size_t max_immutable_memtables{8};
  // Number of background compaction threads.
  std::size_t compaction_thread_count{1};
  // Periodic WAL sync interval in milliseconds (0 disables background sync).
  std::size_t wal_sync_interval_ms{0};
  // Enable CRC32 for WAL records. Disable for performance experiments only.
  bool enable_crc{true};
  // Number of shards in memtable for concurrency (power of two recommended).
  std::size_t memtable_shard_count{16};
  bool enable_compress{false};
};

struct WriteOptions {
  bool sync{false};
};

struct ReadOptions {
  bool fill_cache{true};
  // If true, Scan will build a snapshot view at call time (seq <= snapshot_seq).
  // If snapshot_seq is non-zero, it overrides and uses that sequence boundary.
  bool snapshot{false};
  std::uint64_t snapshot_seq{0};
};

}  // namespace dkv
