#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "bloom.h"

namespace dkv {

// LRU cache for bloom bitmaps (lazy-loaded), keyed by SSTable path.
class BloomCache {
 public:
  struct Data {
    BloomFilter bloom;
    std::size_t bytes{0};
  };

  explicit BloomCache(std::size_t capacity_bytes) : capacity_bytes_(capacity_bytes) {}

  std::shared_ptr<Data> Get(const std::string& file);
  std::shared_ptr<Data> Put(const std::string& file, std::vector<std::uint8_t> bits,
                            std::uint32_t bits_per_key, std::uint32_t k);

 private:
  struct Entry {
    std::string file;
    std::shared_ptr<Data> data;
    std::size_t bytes{0};
  };

  void EvictIfNeeded();

  std::size_t capacity_bytes_{0};
  std::size_t used_bytes_{0};

  std::list<Entry> lru_;
  std::unordered_map<std::string, std::list<Entry>::iterator> map_;
  std::mutex mu_;
};

}  // namespace dkv
