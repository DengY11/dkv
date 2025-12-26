#include "bloom_cache.h"

namespace dkv {

std::shared_ptr<BloomCache::Data> BloomCache::Get(const std::string& file) {
  std::lock_guard lock(mu_);
  auto it = map_.find(file);
  if (it == map_.end()) return nullptr;
  lru_.splice(lru_.begin(), lru_, it->second);
  return it->second->data;
}

std::shared_ptr<BloomCache::Data> BloomCache::Put(const std::string& file, std::vector<std::uint8_t> bits,
                                                  std::uint32_t bits_per_key, std::uint32_t k) {
  const std::size_t bytes = bits.size();
  if (bytes == 0 || capacity_bytes_ == 0 || bytes > capacity_bytes_) return nullptr;

  BloomFilter bloom;
  bloom.SetData(std::move(bits), bits_per_key, k);
  auto data = std::make_shared<Data>(Data{std::move(bloom), bytes});

  std::lock_guard lock(mu_);
  auto it = map_.find(file);
  if (it != map_.end()) {
    used_bytes_ -= it->second->bytes;
    it->second->data = data;
    it->second->bytes = bytes;
    used_bytes_ += bytes;
    lru_.splice(lru_.begin(), lru_, it->second);
  } else {
    lru_.push_front(Entry{file, data, bytes});
    map_[file] = lru_.begin();
    used_bytes_ += bytes;
  }
  EvictIfNeeded();
  return data;
}

void BloomCache::EvictIfNeeded() {
  while (used_bytes_ > capacity_bytes_ && !lru_.empty()) {
    auto last = lru_.end();
    --last;
    used_bytes_ -= last->bytes;
    map_.erase(last->file);
    lru_.pop_back();
  }
}

}  // namespace dkv
