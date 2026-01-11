#include "memtable.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "util.h"

namespace dkv {
namespace {

constexpr int kMaxHeight = 16;

struct Node {
  std::string_view key;
  std::string_view value;
  std::uint64_t seq{0};
  bool deleted{false};
  int height{1};
  std::atomic<Node*> next[1];

  explicit Node(int h) : height(h) {
    for (int i = 0; i < h; ++i) {
      new (&next[i]) std::atomic<Node*>{nullptr};
    }
  }
};

class Arena {
 public:
  explicit Arena(std::size_t initial_bytes) {
    const std::size_t bytes = initial_bytes ? initial_bytes : kDefaultBlockSize;
    auto blk = std::make_unique<Block>();
    blk->size = bytes;
    blk->data = std::make_unique<char[]>(bytes);
    blk->offset.store(0, std::memory_order_relaxed);
    current_.store(blk.get(), std::memory_order_relaxed);
    blocks_.push_back(std::move(blk));
  }

  void* AllocateAligned(std::size_t bytes, std::size_t align = alignof(std::max_align_t)) {
    used_.fetch_add(bytes, std::memory_order_relaxed);
    while (true) {
      Block* b = current_.load(std::memory_order_acquire);
      if (void* ptr = TryAlloc(b, bytes, align)) return ptr;

      std::lock_guard<std::mutex> lk(mu_);
      b = current_.load(std::memory_order_acquire);
      if (void* ptr = TryAlloc(b, bytes, align)) return ptr;

      auto blk = std::make_unique<Block>();
      blk->size = std::max<std::size_t>(bytes + align, kDefaultBlockSize);
      blk->data = std::make_unique<char[]>(blk->size);
      blk->offset.store(0, std::memory_order_relaxed);
      Block* blk_ptr = blk.get();
      blocks_.push_back(std::move(blk));
      current_.store(blk_ptr, std::memory_order_release);
      if (void* ptr = TryAlloc(blk_ptr, bytes, align)) return ptr;
    }
  }

  std::size_t Used() const { return used_.load(std::memory_order_relaxed); }

 private:
  struct Block {
    std::unique_ptr<char[]> data;
    std::size_t size{0};
    std::atomic<std::size_t> offset{0};
  };

  static constexpr std::size_t kDefaultBlockSize = 1 << 20;  // 1 MB default slab

  static std::size_t AlignUp(std::size_t n, std::size_t align) {
    return (n + align - 1) & ~(align - 1);
  }

  void* TryAlloc(Block* b, std::size_t bytes, std::size_t align) {
    if (!b) return nullptr;
    while (true) {
      std::size_t cur = b->offset.load(std::memory_order_relaxed);
      std::size_t aligned = AlignUp(cur, align);
      std::size_t next = aligned + bytes;
      if (next > b->size) return nullptr;
      if (b->offset.compare_exchange_weak(cur, next, std::memory_order_acquire,
                                          std::memory_order_relaxed)) {
        return b->data.get() + aligned;
      }
    }
  }

  std::vector<std::unique_ptr<Block>> blocks_;
  std::atomic<Block*> current_{nullptr};
  std::atomic<std::size_t> used_{0};
  std::mutex mu_;
};

class SkipList {
 public:
  explicit SkipList(Arena* arena) : arena_(arena) {
    head_ = NewNode("", "", 0, false, kMaxHeight);
    for (int i = 0; i < kMaxHeight; ++i) {
      head_->SetNext(i, nullptr);
    }
    max_height_.store(1, std::memory_order_relaxed);
  }

  void Insert(std::string_view key, std::string_view value, std::uint64_t seq, bool deleted) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, seq, prev);

    int height = RandomHeight();
    int cur_max = max_height_.load(std::memory_order_relaxed);
    if (height > cur_max) {
      for (int i = cur_max; i < height; ++i) {
        prev[i] = head_;
      }
      max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, value, seq, deleted, height);
    for (int i = 0; i < height; ++i) {
      x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
      prev[i]->SetNext(i, x);
    }
  }

  bool GetLatest(std::string_view key, MemEntry& out) const {
    Node* x = FindGreaterOrEqual(key, std::numeric_limits<std::uint64_t>::max(), nullptr);
    if (!x || x->key != key) return false;
    out.key = std::string(x->key);
    out.value = std::string(x->value);
    out.seq = x->seq;
    out.deleted = x->deleted;
    return true;
  }

  void SnapshotViews(std::vector<MemEntryView>& out) const {
    Node* x = head_->Next(0);
    while (x != nullptr) {
      out.push_back(MemEntryView{x->key, x->value, x->seq, x->deleted});
      x = x->Next(0);
    }
  }

  bool Empty() const { return head_->Next(0) == nullptr; }

 private:
  struct Node {
    std::string_view key;
    std::string_view value;
    std::uint64_t seq{0};
    bool deleted{false};
    explicit Node(int height) : height_(height) {}

    Node* Next(int n) {
      assert(n >= 0);
      return next_[n].load(std::memory_order_acquire);
    }
    void SetNext(int n, Node* x) {
      assert(n >= 0);
      next_[n].store(x, std::memory_order_release);
    }
    Node* NoBarrier_Next(int n) {
      assert(n >= 0);
      return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
      assert(n >= 0);
      next_[n].store(x, std::memory_order_relaxed);
    }

    int height() const { return height_; }

   private:
    int height_;
    std::atomic<Node*> next_[1];
  };

  Node* NewNode(std::string_view key, std::string_view value, std::uint64_t seq, bool deleted, int height) {
    char* mem =
        static_cast<char*>(arena_->AllocateAligned(sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1), alignof(Node)));
    auto* n = new (mem) Node(height);
    n->seq = seq;
    n->deleted = deleted;

    char* kbuf = static_cast<char*>(arena_->AllocateAligned(key.size(), alignof(char)));
    std::memcpy(kbuf, key.data(), key.size());
    n->key = std::string_view(kbuf, key.size());
    if (!value.empty()) {
      char* vbuf = static_cast<char*>(arena_->AllocateAligned(value.size(), alignof(char)));
      std::memcpy(vbuf, value.data(), value.size());
      n->value = std::string_view(vbuf, value.size());
    }
    return n;
  }

  // Comparator: order by key ascending, then seq descending (higher seq first).
  int Compare(const Node* a, std::string_view b_key, std::uint64_t b_seq) const {
    int cmp = a->key.compare(b_key);
    if (cmp == 0) {
      if (a->seq == b_seq) return 0;
      return a->seq > b_seq ? -1 : 1;
    }
    return cmp;
  }

  bool KeyIsAfterNode(std::string_view key, std::uint64_t seq, Node* n) const {
    return (n != nullptr) && (Compare(n, key, seq) < 0);
  }

  Node* FindGreaterOrEqual(std::string_view key, std::uint64_t seq, Node** prev) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
      Node* next = x->Next(level);
      if (KeyIsAfterNode(key, seq, next)) {
        x = next;
      } else {
        if (prev != nullptr) prev[level] = x;
        if (level == 0) {
          return next;
        }
        --level;
      }
    }
  }

  int RandomHeight() {
    static thread_local std::mt19937 rng(std::random_device{}());
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());
    int height = 1;
    while (height < kMaxHeight && (dist(rng) & 3) == 0) {  // 1/4 branching like LevelDB
      ++height;
    }
    return height;
  }

  Arena* arena_;
  Node* head_{nullptr};
  std::atomic<int> max_height_{1};
};

}  // namespace

struct MemTable::Impl {
  explicit Impl(std::size_t approx_bytes)
      : arena(std::make_unique<Arena>(approx_bytes)), list(std::make_unique<SkipList>(arena.get())) {}

  std::unique_ptr<Arena> arena;
  std::unique_ptr<SkipList> list;
  std::mutex write_mu;
};

MemTable::MemTable(std::size_t approx_capacity_bytes) : impl_(std::make_unique<Impl>(approx_capacity_bytes)) {}

MemTable::~MemTable() = default;

Status MemTable::Put(std::uint64_t seq, std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> lk(impl_->write_mu);
  impl_->list->Insert(key, value, seq, false);
  return Status::OK();
}

Status MemTable::Delete(std::uint64_t seq, std::string_view key) {
  std::lock_guard<std::mutex> lk(impl_->write_mu);
  impl_->list->Insert(key, std::string_view{}, seq, true);
  return Status::OK();
}

bool MemTable::Get(std::string_view key, MemEntry& entry) const {
  return impl_->list->GetLatest(key, entry);
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
  std::vector<MemEntryView> out;
  impl_->list->SnapshotViews(out);
  return out;
}

void MemTable::Clear() {
  const std::size_t approx = impl_->arena->Used();
  impl_ = std::make_unique<Impl>(approx);
}

std::size_t MemTable::ApproximateMemoryUsage() const { return impl_->arena->Used(); }

bool MemTable::Empty() const { return impl_->list->Empty(); }

}  // namespace dkv
