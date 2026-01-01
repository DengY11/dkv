#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <atomic>
#include <unordered_map>
#include <vector>

#include "memtable.h"

namespace dkv {

class BlockCache {
 public:
  explicit BlockCache(std::size_t capacity_bytes) : capacity_bytes_(capacity_bytes) {}

  bool Get(const std::shared_ptr<const std::string>& file, std::uint64_t offset,
           std::shared_ptr<const std::vector<MemEntry>>& out);
  void Put(const std::shared_ptr<const std::string>& file, std::uint64_t offset,
           std::vector<MemEntry> entries);

  struct Stats {
    std::uint64_t hits{0};
    std::uint64_t misses{0};
    std::uint64_t puts{0};
    std::uint64_t evictions{0};
    std::size_t used_bytes{0};
    std::size_t capacity_bytes{0};
    std::size_t entries{0};
  };
  Stats GetStats() const;

 private:
  struct Key {
    const std::string* file{nullptr};
    std::uint64_t offset;

    bool operator==(const Key& other) const { return offset == other.offset && file == other.file; }
  };

  struct KeyHash {
    std::size_t operator()(const Key& k) const noexcept {
      return std::hash<const void*>()(k.file) ^ (std::hash<std::uint64_t>()(k.offset) << 1);
    }
  };

  struct Entry {
    Key key;
    std::shared_ptr<const std::string> file_ref;
    std::shared_ptr<const std::vector<MemEntry>> entries;
    std::size_t bytes{0};
  };

  void EvictIfNeeded();

  std::size_t capacity_bytes_{0};
  std::size_t used_bytes_{0};

  std::list<Entry> lru_;
  std::unordered_map<Key, std::list<Entry>::iterator, KeyHash> map_;
  mutable std::mutex mu_;

  std::atomic<std::uint64_t> hits_{0};
  std::atomic<std::uint64_t> misses_{0};
  std::atomic<std::uint64_t> puts_{0};
  std::atomic<std::uint64_t> evictions_{0};
};

}  // namespace dkv
