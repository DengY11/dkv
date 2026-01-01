#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "block_cache.h"
#include "bloom_cache.h"
#include "dkv/options.h"
#include "memtable.h"
#include "bloom.h"
#include "util.h"

namespace dkv {

class SSTable {
 public:
  static Status Write(const std::filesystem::path& path, const std::vector<MemEntry>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, bool enable_compress);
  static Status Write(const std::filesystem::path& path, const std::vector<MemEntryView>& entries,
                     std::size_t block_size, std::size_t bloom_bits_per_key, bool enable_compress);
  static Status Open(const std::filesystem::path& path, const std::shared_ptr<BlockCache>& cache,
                    const std::shared_ptr<BloomCache>& bloom_cache, bool pin_bloom,
                    std::shared_ptr<SSTable>& out);

  bool Get(std::string_view key, MemEntry& entry) const;
  Status LoadAll(std::vector<MemEntry>& out) const;
  Status Scan(std::string_view from, std::size_t limit,
              std::vector<std::pair<std::string, std::string>>& out) const;

  [[nodiscard]] const std::filesystem::path& path() const { return path_; }
  [[nodiscard]] std::uint64_t max_sequence() const { return max_seq_; }
  [[nodiscard]] const std::string& min_key() const { return min_key_; }
  [[nodiscard]] const std::string& max_key() const { return max_key_; }
  [[nodiscard]] std::uint64_t file_size() const { return file_size_; }
  [[nodiscard]] std::size_t block_count() const { return blocks_.size(); }
  Status ReadBlockByIndex(std::size_t index, std::vector<MemEntry>& out) const;

 private:
  struct BlockIndexEntry {
    std::string key;
    std::uint64_t offset{0};
    std::uint32_t size{0};
  };

  SSTable(std::filesystem::path path, std::vector<BlockIndexEntry> index, std::string min_key,
          std::string max_key, std::uint64_t max_seq, std::uint64_t file_size, std::uint64_t bloom_start,
          std::uint32_t bloom_bytes, std::uint32_t bloom_bits_per_key, bool pin_bloom,
          std::shared_ptr<BlockCache> cache, std::shared_ptr<BloomCache> bloom_cache);

  std::shared_ptr<BloomCache::Data> LoadBloom() const;

  bool ReadEntryRange(std::uint64_t start, std::uint64_t end, std::string_view key, MemEntry& entry) const;
  bool ReadBlockRange(std::uint64_t start, std::uint64_t end,
                      std::vector<std::pair<std::string, std::string>>& out, std::size_t limit) const;
  bool ReadBlock(std::uint64_t start, std::uint64_t size,
                 std::shared_ptr<const std::vector<MemEntry>>& out) const;

  std::filesystem::path path_;
  std::shared_ptr<const std::string> path_ref_;
  std::vector<BlockIndexEntry> blocks_;
  std::string min_key_;
  std::string max_key_;
  std::uint64_t max_seq_;
  std::uint64_t file_size_;
  std::uint64_t bloom_start_;
  std::shared_ptr<BlockCache> cache_;
  std::shared_ptr<BloomCache> bloom_cache_;
  std::uint32_t bloom_bytes_{0};
  std::uint32_t bloom_bits_per_key_{0};
  bool pin_bloom_{false};
  mutable std::weak_ptr<BloomCache::Data> bloom_ref_;
  mutable std::shared_ptr<BloomCache::Data> pinned_bloom_;
  mutable std::ifstream file_;
  mutable std::mutex io_mu_;
};

}  // namespace dkv
