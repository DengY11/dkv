#include "block_cache.h"

#include <numeric>

namespace dkv {

bool BlockCache::Get(const std::shared_ptr<const std::string>& file, std::uint64_t offset,
                     std::shared_ptr<const std::vector<MemEntry>>& out) {
  std::lock_guard lock(mu_);
  Key key{file.get(), offset};
  auto it = map_.find(key);
  if (it == map_.end()) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  lru_.splice(lru_.begin(), lru_, it->second);
  out = it->second->entries;
  hits_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void BlockCache::Put(const std::shared_ptr<const std::string>& file, std::uint64_t offset,
                     std::vector<MemEntry> entries) {
  std::size_t bytes = 0;
  for (const auto& e : entries) {
    bytes += 1 + sizeof(std::uint64_t) + sizeof(std::uint32_t) * 2 + e.key.size() + e.value.size();
  }
  if (bytes > capacity_bytes_) return;  // too big to cache

  std::lock_guard lock(mu_);
  Key key{file.get(), offset};
  auto sp = std::make_shared<std::vector<MemEntry>>(std::move(entries));
  auto existing = map_.find(key);
  if (existing != map_.end()) {
    used_bytes_ -= existing->second->bytes;
    existing->second->entries = sp;
    existing->second->bytes = bytes;
    existing->second->file_ref = file;
    used_bytes_ += bytes;
    lru_.splice(lru_.begin(), lru_, existing->second);
  } else {
    lru_.push_front(Entry{key, file, std::move(sp), bytes});
    map_[key] = lru_.begin();
    used_bytes_ += bytes;
  }
  puts_.fetch_add(1, std::memory_order_relaxed);
  EvictIfNeeded();
}

void BlockCache::EvictIfNeeded() {
  while (used_bytes_ > capacity_bytes_ && !lru_.empty()) {
    auto last = lru_.end();
    --last;
    used_bytes_ -= last->bytes;
    map_.erase(last->key);
    lru_.pop_back();
    evictions_.fetch_add(1, std::memory_order_relaxed);
  }
}

BlockCache::Stats BlockCache::GetStats() const {
  std::lock_guard lock(mu_);
  Stats s;
  s.hits = hits_.load(std::memory_order_relaxed);
  s.misses = misses_.load(std::memory_order_relaxed);
  s.puts = puts_.load(std::memory_order_relaxed);
  s.evictions = evictions_.load(std::memory_order_relaxed);
  s.used_bytes = used_bytes_;
  s.capacity_bytes = capacity_bytes_;
  s.entries = lru_.size();
  return s;
}

}  // namespace dkv
