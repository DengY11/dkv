#include "memtable.h"

#include <algorithm>
#include <mutex>
#include <utility>

namespace dkv {

Status MemTable::Put(std::uint64_t seq, std::string_view key, std::string_view value) {
  std::unique_lock lock(mu_);
  auto it = table_.find(key);
  if (it != table_.end()) {
    memory_usage_ -= it->second.value.size();
    it->second.value.assign(value.data(), value.size());
    it->second.seq = seq;
    it->second.deleted = false;
    memory_usage_ += it->second.value.size();
  } else {
    auto* res = table_.get_allocator().resource();
    std::pmr::string key_copy{key, res};
    std::pmr::string value_copy{value, res};
    memory_usage_ += key_copy.size() + value_copy.size();
    table_.emplace(std::move(key_copy), MemValue{std::move(value_copy), seq, false});
  }
  return Status::OK();
}

Status MemTable::Delete(std::uint64_t seq, std::string_view key) {
  std::unique_lock lock(mu_);
  auto it = table_.find(key);
  if (it != table_.end()) {
    memory_usage_ -= it->second.value.size();
    it->second.value.clear();
    it->second.seq = seq;
    it->second.deleted = true;
  } else {
    auto* res = table_.get_allocator().resource();
    std::pmr::string key_copy{key, res};
    memory_usage_ += key_copy.size();
    table_.emplace(std::move(key_copy), MemValue{std::pmr::string(res), seq, true});
  }
  return Status::OK();
}

bool MemTable::Get(std::string_view key, MemEntry& entry) const {
  std::shared_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) return false;
  entry.key = it->first;
  entry.value = it->second.value;
  entry.seq = it->second.seq;
  entry.deleted = it->second.deleted;
  return true;
}

std::vector<MemEntry> MemTable::Snapshot() const {
  std::shared_lock lock(mu_);
  std::vector<MemEntry> out;
  out.reserve(table_.size());
  for (const auto& kv : table_) {
    out.push_back(MemEntry{std::string(kv.first), std::string(kv.second.value), kv.second.seq,
                           kv.second.deleted});
  }
  return out;
}

void MemTable::Clear() {
  std::unique_lock lock(mu_);
  table_.clear();
  buffer_.release();
  table_ = decltype(table_)(&buffer_);
  memory_usage_ = 0;
}

std::size_t MemTable::ApproximateMemoryUsage() const {
  std::shared_lock lock(mu_);
  return memory_usage_;
}

bool MemTable::Empty() const {
  std::shared_lock lock(mu_);
  return table_.empty();
}

}  // namespace dkv
