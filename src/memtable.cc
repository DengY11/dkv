#include "memtable.h"

#include <algorithm>
#include <functional>
#include <mutex>
#include <memory>
#include <queue>
#include <utility>
#include <new>
#include <vector>

namespace dkv {

namespace {
inline std::size_t KeyHash(std::string_view key) { return std::hash<std::string_view>{}(key); }

struct TransparentHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view key) const { return std::hash<std::string_view>{}(key); }
};

struct TransparentEq {
  using is_transparent = void;
  bool operator()(std::string_view a, std::string_view b) const { return a == b; }
};

class ReusableMonotonicResource : public std::pmr::memory_resource {
 public:
  explicit ReusableMonotonicResource(std::size_t block_size, void* initial_buffer, std::size_t initial_size)
      : block_size_(block_size) {
    blocks_.push_back(Block{static_cast<std::byte*>(initial_buffer), initial_size, 0, true});
  }

  void Release() {
    for (std::size_t i = 0; i < blocks_.size(); ++i) {
      blocks_[i].offset = 0;
      if (i > 0 && !blocks_[i].external) {
        delete[] blocks_[i].data;
        blocks_[i].data = nullptr;
        blocks_[i].size = 0;
      }
    }
    if (blocks_.size() > 1) blocks_.resize(1);
  }

 protected:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    for (auto& blk : blocks_) {
      void* ptr = blk.data + blk.offset;
      std::size_t space = blk.size - blk.offset;
      void* aligned = ptr;
      if (std::align(alignment, bytes, aligned, space)) {
        blk.offset = static_cast<std::size_t>(static_cast<std::byte*>(aligned) - blk.data) + bytes;
        return aligned;
      }
    }
    // Need new block
    std::size_t sz = std::max(block_size_, bytes + alignment);
    auto* buf = new std::byte[sz];
    blocks_.push_back(Block{buf, sz, 0, false});
    void* ptr = buf;
    std::size_t space = sz;
    void* aligned = ptr;
    bool ok = std::align(alignment, bytes, aligned, space);
    (void)ok;
    blocks_.back().offset = static_cast<std::size_t>(static_cast<std::byte*>(aligned) - buf) + bytes;
    return aligned;
  }

  void do_deallocate(void*, std::size_t, std::size_t) override {}
  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

 private:
  struct Block {
    std::byte* data{nullptr};
    std::size_t size{0};
    std::size_t offset{0};
    bool external{false};
  };

  std::size_t block_size_;
  std::vector<Block> blocks_;
};
}  // namespace

struct MemTable::Shard {
  explicit Shard(std::size_t reserve_buckets)
      : resource_(kInlineArenaSize, inline_arena_.data(), inline_arena_.size()),
        table_(0, TransparentHash{}, TransparentEq{},
               std::pmr::polymorphic_allocator<std::pair<const std::pmr::string, MemValue>>{&resource_}) {
    if (reserve_buckets > 0) table_.reserve(reserve_buckets);
  }

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

  void SnapshotViews(std::vector<MemEntryView>& out) const {
    std::shared_lock lk(mu_);
    out.reserve(out.size() + table_.size());
    for (const auto& kv : table_) {
      out.push_back(MemEntryView{std::string_view(kv.first), std::string_view(kv.second.value), kv.second.seq,
                                 kv.second.deleted});
    }
  }

  void Clear() {
    std::unique_lock lk(mu_);
    table_.clear();
    resource_.Release();
    table_ = decltype(table_)(0, TransparentHash{}, TransparentEq{},
                              std::pmr::polymorphic_allocator<std::pair<const std::pmr::string, MemValue>>{&resource_});
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
  ReusableMonotonicResource resource_;
  std::pmr::unordered_map<std::pmr::string, MemValue, TransparentHash, TransparentEq> table_;
  std::size_t memory_usage_{0};
};

MemTable::MemTable(std::size_t shard_count, std::size_t approx_capacity_bytes)
    : shard_count_(shard_count == 0 ? 1 : shard_count) {
  shards_.reserve(shard_count_);
  constexpr std::size_t kMinBucketsPerShard = 1 << 15;
  constexpr std::size_t kApproxEntryBytes = 64;  // rough key+value size for bucket sizing
  std::size_t reserve_buckets = kMinBucketsPerShard;
  if (approx_capacity_bytes > 0) {
    const auto approx_entries = approx_capacity_bytes / kApproxEntryBytes;
    const auto per_shard = approx_entries / shard_count_;
    if (per_shard > reserve_buckets) {
      reserve_buckets = per_shard;
    }
  }
  for (std::size_t i = 0; i < shard_count_; ++i) {
    shards_.push_back(std::make_unique<Shard>(reserve_buckets));
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
  auto views = SnapshotViews();
  std::vector<MemEntry> out;
  out.reserve(views.size());
  for (const auto& v : views) {
    out.push_back(MemEntry{std::string(v.key), std::string(v.value), v.seq, v.deleted});
  }
  return out;
}

std::vector<MemEntryView> MemTable::SnapshotViews() const {
  struct ShardBuf {
    std::vector<MemEntryView> entries;
  };
  std::vector<ShardBuf> shards_sorted;
  shards_sorted.reserve(shard_count_);
  std::size_t total = 0;

  for (const auto& shard : shards_) {
    ShardBuf buf;
    shard->SnapshotViews(buf.entries);
    std::sort(buf.entries.begin(), buf.entries.end(),
              [](const MemEntryView& a, const MemEntryView& b) { return a.key < b.key; });
    total += buf.entries.size();
    shards_sorted.push_back(std::move(buf));
  }

  std::vector<MemEntryView> out;
  out.reserve(total);
  struct HeapItem {
    std::size_t shard_idx;
    std::size_t elem_idx;
  };
  auto cmp = [&](const HeapItem& a, const HeapItem& b) {
    return shards_sorted[a.shard_idx].entries[a.elem_idx].key >
           shards_sorted[b.shard_idx].entries[b.elem_idx].key;
  };
  std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmp)> heap(cmp);
  for (std::size_t i = 0; i < shards_sorted.size(); ++i) {
    if (!shards_sorted[i].entries.empty()) heap.push(HeapItem{i, 0});
  }

  while (!heap.empty()) {
    auto cur = heap.top();
    heap.pop();
    const auto& e = shards_sorted[cur.shard_idx].entries[cur.elem_idx];
    out.push_back(e);
    ++cur.elem_idx;
    if (cur.elem_idx < shards_sorted[cur.shard_idx].entries.size()) {
      heap.push(cur);
    }
  }
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
