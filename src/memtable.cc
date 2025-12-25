#include "memtable.h"

#include <algorithm>
#include <functional>
#include <mutex>
#include <utility>

namespace dkv {

namespace {
inline std::size_t KeyHash(std::string_view key) { return std::hash<std::string_view>{}(key); }
}  // namespace

struct MemTable::Shard {
  Shard() : buffer_(inline_arena_.data(), inline_arena_.size()), table_{&buffer_} {}

  Status Put(std::uint64_t seq, std::string_view key, std::string_view value) {
    std::unique_lock lk(mu_);
    auto it = table_.find(key);
    if (it != table_.end()) {
      memory_usage_ -= it->second.value.size();
      it->second.value.assign(value.data(), value.size());
      it->second.seq = seq;
      it->second.deleted = false;
      memory_usage_ += it->second.value.size();
    } else {
      auto* res = table_.get_allocator().resource();
      std::pmr::string key_copy(key.begin(), key.end(), res);
      std::pmr::string value_copy(value.begin(), value.end(), res);
      memory_usage_ += key_copy.size() + value_copy.size();
      table_.emplace(std::move(key_copy), MemValue{std::move(value_copy), seq, false});
    }
    return Status::OK();
  }

  Status Delete(std::uint64_t seq, std::string_view key) {
    std::unique_lock lk(mu_);
    auto it = table_.find(key);
    if (it != table_.end()) {
      memory_usage_ -= it->second.value.size();
      it->second.value.clear();
      it->second.seq = seq;
      it->second.deleted = true;
    } else {
      auto* res = table_.get_allocator().resource();
      std::pmr::string key_copy(key.begin(), key.end(), res);
      memory_usage_ += key_copy.size();
      table_.emplace(std::move(key_copy), MemValue{std::pmr::string(res), seq, true});
    }
    return Status::OK();
  }

  bool Get(std::string_view key, MemEntry& entry) const {
    std::shared_lock lk(mu_);
    auto it = table_.find(key);
    if (it == table_.end()) return false;
    entry.key = it->first;
    entry.value = it->second.value;
    entry.seq = it->second.seq;
    entry.deleted = it->second.deleted;
    return true;
  }

  void Snapshot(std::vector<MemEntry>& out) const {
    std::shared_lock lk(mu_);
    out.reserve(out.size() + table_.size());
    for (const auto& kv : table_) {
      out.push_back(MemEntry{std::string(kv.first), std::string(kv.second.value), kv.second.seq,
                             kv.second.deleted});
    }
  }

  void Clear() {
    std::unique_lock lk(mu_);
    table_.clear();
    buffer_.release();
    table_ = decltype(table_)(&buffer_);
    memory_usage_ = 0;
  }

  std::size_t ApproximateMemoryUsage() const {
    std::shared_lock lk(mu_);
    return memory_usage_;
  }

  bool Empty() const {
    std::shared_lock lk(mu_);
    return table_.empty();
  }

  mutable std::shared_mutex mu_;
  static constexpr std::size_t kInlineArenaSize = 256 * 1024;
  alignas(std::max_align_t) std::array<std::byte, kInlineArenaSize> inline_arena_{};
  std::pmr::monotonic_buffer_resource buffer_;
  std::pmr::map<std::pmr::string, MemValue, std::less<>> table_;
  std::size_t memory_usage_{0};
};

MemTable::MemTable(std::size_t shard_count) : shard_count_(shard_count) {
  shards_.reserve(shard_count_);
  for (std::size_t i = 0; i < shard_count_; ++i) {
    shards_.push_back(std::make_unique<Shard>());
  }
}

MemTable::~MemTable() = default;

Status MemTable::Put(std::uint64_t seq, std::string_view key, std::string_view value) {
  auto shard = shards_[KeyHash(key) % shard_count_].get();
  return shard->Put(seq, key, value);
}

Status MemTable::Delete(std::uint64_t seq, std::string_view key) {
  auto shard = shards_[KeyHash(key) % shard_count_].get();
  return shard->Delete(seq, key);
}

bool MemTable::Get(std::string_view key, MemEntry& entry) const {
  auto shard = shards_[KeyHash(key) % shard_count_].get();
  return shard->Get(key, entry);
}

std::vector<MemEntry> MemTable::Snapshot() const {
  std::vector<MemEntry> out;
  for (const auto& shard : shards_) {
    shard->Snapshot(out);
  }
  std::sort(out.begin(), out.end(), [](const MemEntry& a, const MemEntry& b) {
    if (a.key == b.key) return a.seq > b.seq;
    return a.key < b.key;
  });
  return out;
}

void MemTable::Clear() {
  for (auto& shard : shards_) {
    shard->Clear();
  }
}

std::size_t MemTable::ApproximateMemoryUsage() const {
  std::size_t total = 0;
  for (const auto& shard : shards_) {
    total += shard->ApproximateMemoryUsage();
  }
  return total;
}

bool MemTable::Empty() const {
  for (const auto& shard : shards_) {
    if (!shard->Empty()) return false;
  }
  return true;
}

}  // namespace dkv
