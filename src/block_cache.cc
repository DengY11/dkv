#include "block_cache.h"

#include <numeric>

namespace dkv {

bool BlockCache::Get(const std::string& file, std::uint64_t offset, std::vector<MemEntry>& out) {
  std::lock_guard lock(mu_);
  Key key{file, offset};
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  lru_.splice(lru_.begin(), lru_, it->second);
  out = it->second->entries;
  return true;
}

void BlockCache::Put(const std::string& file, std::uint64_t offset, std::vector<MemEntry> entries) {
  std::size_t bytes = 0;
  for (const auto& e : entries) {
    bytes += 1 + sizeof(std::uint64_t) + sizeof(std::uint32_t) * 2 + e.key.size() + e.value.size();
  }
  if (bytes > capacity_bytes_) return;  // too big to cache

  std::lock_guard lock(mu_);
  Key key{file, offset};
  auto existing = map_.find(key);
  if (existing != map_.end()) {
    used_bytes_ -= existing->second->bytes;
    existing->second->entries = std::move(entries);
    existing->second->bytes = bytes;
    used_bytes_ += bytes;
    lru_.splice(lru_.begin(), lru_, existing->second);
  } else {
    lru_.push_front(Entry{key, std::move(entries), bytes});
    map_[key] = lru_.begin();
    used_bytes_ += bytes;
  }
  EvictIfNeeded();
}

void BlockCache::EvictIfNeeded() {
  while (used_bytes_ > capacity_bytes_ && !lru_.empty()) {
    auto last = lru_.end();
    --last;
    used_bytes_ -= last->bytes;
    map_.erase(last->key);
    lru_.pop_back();
  }
}

}  // namespace dkv
