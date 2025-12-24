#pragma once

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "memtable.h"

namespace dkv {

class BlockCache {
 public:
  explicit BlockCache(std::size_t capacity_bytes) : capacity_bytes_(capacity_bytes) {}

  bool Get(const std::string& file, std::uint64_t offset, std::vector<MemEntry>& out);
  void Put(const std::string& file, std::uint64_t offset, std::vector<MemEntry> entries);

 private:
  struct Key {
    std::string file;
    std::uint64_t offset;

    bool operator==(const Key& other) const { return offset == other.offset && file == other.file; }
  };

  struct KeyHash {
    std::size_t operator()(const Key& k) const noexcept {
      return std::hash<std::string>()(k.file) ^ (std::hash<std::uint64_t>()(k.offset) << 1);
    }
  };

  struct Entry {
    Key key;
    std::vector<MemEntry> entries;
    std::size_t bytes{0};
  };

  void EvictIfNeeded();

  std::size_t capacity_bytes_{0};
  std::size_t used_bytes_{0};

  std::list<Entry> lru_;
  std::unordered_map<Key, std::list<Entry>::iterator, KeyHash> map_;
  std::mutex mu_;
};

}  // namespace dkv
