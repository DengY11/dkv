#include "dkv/db.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cinttypes>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <atomic>
#include <algorithm>
#include <deque>
#include "block_cache.h"
#include "bloom_cache.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"
#include "util.h"

namespace dkv {

struct DB::Iterator::Rep {
  std::vector<std::pair<std::string, std::string>> data;
  std::size_t pos{0};
  Status status{Status::OK()};
  std::string prefix;
  std::uint64_t snapshot_seq{0};
  bool registered{false};
  Impl* owner{nullptr};
};

class DB::Impl {
 public:
  explicit Impl(Options options)
      : options_(std::move(options)),
        data_dir_(options_.data_dir),
        wal_path_(data_dir_ / "wal.log"),
        sst_dir_(data_dir_ / "sst"),
        mem_(std::make_unique<MemTable>(options_.memtable_shard_count, options_.memtable_soft_limit_bytes)) {}
  ~Impl();

  Status Init();

  Status Put(const WriteOptions& options, std::string key, std::string value);
  Status Delete(const WriteOptions& options, std::string key);
  Status Write(const WriteOptions& options, const WriteBatch& batch);
  Status Get(const ReadOptions& options, std::string_view key, std::string& value);
  Status Flush();
  Status Compact();
  std::unique_ptr<DB::Iterator> NewIterator(const ReadOptions& options, std::string_view prefix);
  Metrics GetMetrics() const;

 private:
  friend class DB::Iterator;
  struct TableRef;
  struct ImmutableMem;
  void StartWalSyncThread();
  void StopWalSyncThread();
  void WalSyncLoop();
  Status FlushLocked();
  Status LoadSSTables();
  Status LoadManifest(std::vector<std::vector<TableRef>>& loaded);
  Status WriteManifest();
  Status CompactLevel(std::size_t level);
  std::uint64_t LevelMaxBytes(std::size_t level) const;
  std::uint64_t LevelBytes(std::size_t level) const;
  Status RotateWalLocked(std::uint64_t max_seq_for_old, std::filesystem::path& rotated_path);
  void MaybeScheduleFlush();
  void FlushThreadLoop();
  void ApplyThreadLoop();
  void CompactThreadLoop();
  void EnqueueImmutable(std::unique_ptr<MemTable> mem, std::uint64_t max_seq, std::filesystem::path wal_path);
  Status FlushImmutable(const std::shared_ptr<ImmutableMem>& imm);
  Status BuildMergedView(std::map<std::string, MemEntry>& merged, std::uint64_t snapshot_seq);
  std::uint64_t SnapshotSeq(const ReadOptions& options) const;
  std::uint64_t RegisterSnapshot(std::uint64_t seq);
  void UnregisterSnapshot(std::uint64_t seq);
  std::uint64_t MinActiveSnapshot() const;
  void MaybeScheduleCompaction();
  void EnqueueCompaction(std::size_t level);
  bool PickCompactionLevel(std::size_t& level);
  Status WaitForAllCompactions();

  Options options_;
  std::filesystem::path data_dir_;
  std::filesystem::path wal_path_;
  std::filesystem::path sst_dir_;

  std::unique_ptr<WAL> wal_;
  std::unique_ptr<MemTable> mem_;
  struct ImmutableMem {
    std::unique_ptr<MemTable> mem;
    std::filesystem::path wal_path;
    std::uint64_t max_seq{0};
    bool flushing{false};
    bool flushed{false};
    bool applied{false};
  };
  std::deque<std::shared_ptr<ImmutableMem>> immutables_;
  std::vector<std::thread> flush_threads_;
  std::condition_variable flush_cv_;
  std::mutex flush_mu_;
  bool stop_flush_{false};
  std::size_t in_flight_flush_{0};
  std::shared_ptr<BlockCache> block_cache_;
  std::shared_ptr<BloomCache> bloom_cache_;
  std::mutex manifest_mu_;
  std::atomic<std::uint64_t> manifest_tmp_seq_{0};
  struct FlushResult {
    std::vector<TableRef> tables;
    std::filesystem::path wal_path;
    std::uint64_t total_bytes{0};
    std::uint64_t flush_ms{0};
    std::shared_ptr<ImmutableMem> imm;
  };
  std::deque<FlushResult> apply_queue_;
  std::mutex apply_mu_;
  std::condition_variable apply_cv_;
  bool stop_apply_{false};
  std::thread apply_thread_;
  bool apply_thread_started_{false};
  std::atomic<std::size_t> pending_apply_{0};
  std::vector<std::thread> compact_threads_;
  std::condition_variable compact_cv_;
  std::mutex compact_mu_;
  bool stop_compact_{false};
  std::deque<std::size_t> compact_queue_;
  std::size_t compact_inflight_{0};
  std::vector<bool> compact_scheduled_;
  mutable std::mutex compact_status_mu_;
  Status last_compact_status_{Status::OK()};

  std::shared_mutex mu_;  // shared for reads/writes; unique for flush/compaction/wal rotation.
  std::atomic<std::uint64_t> next_seq_{1};

  std::thread wal_sync_thread_;
  std::condition_variable wal_sync_cv_;
  std::mutex wal_sync_mu_;
  bool stop_wal_sync_{false};

  mutable std::mutex snapshot_mu_;
  std::multiset<std::uint64_t> snapshots_;

  struct MetricsCounters {
    std::atomic<std::uint64_t> puts{0};
    std::atomic<std::uint64_t> deletes{0};
    std::atomic<std::uint64_t> gets{0};
    std::atomic<std::uint64_t> batches{0};
    std::atomic<std::uint64_t> flushes{0};
    std::atomic<std::uint64_t> flush_ms{0};
    std::atomic<std::uint64_t> flush_bytes{0};
    std::atomic<std::uint64_t> compactions{0};
    std::atomic<std::uint64_t> compaction_ms{0};
    std::atomic<std::uint64_t> compaction_input_bytes{0};
    std::atomic<std::uint64_t> compaction_output_bytes{0};
    std::atomic<std::uint64_t> wal_syncs{0};
  } metrics_;

  struct TableRef {
    std::shared_ptr<SSTable> table;
    std::string min_key;
    std::string max_key;
    std::uint64_t size{0};
  };

  mutable std::shared_mutex sstable_mu_;
  std::vector<std::vector<TableRef>> levels_;  // level 0 may overlap; level>=1 sorted non-overlap.
  std::vector<std::size_t> next_compact_index_;
};

Status DB::Impl::Init() {
  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);

  wal_ = std::make_unique<WAL>(wal_path_, options_.sync_wal, options_.enable_crc);
  Status s = wal_->Open();
  if (!s.ok()) return s;

  if (options_.block_cache_capacity_bytes > 0) {
    block_cache_ = std::make_shared<BlockCache>(options_.block_cache_capacity_bytes);
  }
  if (options_.bloom_cache_capacity_bytes > 0) {
    bloom_cache_ = std::make_shared<BloomCache>(options_.bloom_cache_capacity_bytes);
  }

  s = LoadSSTables();
  if (!s.ok()) return s;

  std::vector<std::pair<std::uint64_t, std::filesystem::path>> wal_segments;
  if (std::filesystem::exists(wal_path_)) wal_segments.emplace_back(UINT64_MAX, wal_path_);
  for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
    if (!entry.is_regular_file()) continue;
    const auto name = entry.path().filename().string();
    if (name.rfind("wal-", 0) == 0 && entry.path().extension() == ".log") {
      auto num_str = name.substr(4, name.size() - 4 - 4);  // strip wal- and .log
      try {
        auto seq = static_cast<std::uint64_t>(std::stoull(num_str));
        wal_segments.emplace_back(seq, entry.path());
      } catch (...) {
        continue;
      }
    }
  }
  std::sort(wal_segments.begin(), wal_segments.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

  std::uint64_t max_seq_seen = 0;
  for (const auto& seg : wal_segments) {
    WAL reader(seg.second, options_.sync_wal, options_.enable_crc);
    Status rs = reader.Replay([this, &max_seq_seen](std::uint64_t seq, bool deleted, std::string&& key,
                                                    std::string&& value) {
      max_seq_seen = std::max(max_seq_seen, seq);
      if (deleted) {
        mem_->Delete(seq, key);
      } else {
        mem_->Put(seq, key, value);
      }
    });
    if (!rs.ok()) return rs;
  }

  {
    std::shared_lock lock(sstable_mu_);
    for (const auto& level : levels_) {
      for (const auto& t : level) {
        max_seq_seen = std::max(max_seq_seen, t.table->max_sequence());
      }
    }
  }

  next_seq_.store(max_seq_seen + 1, std::memory_order_relaxed);
  StartWalSyncThread();
  stop_flush_ = false;
  const std::size_t flush_threads = std::max<std::size_t>(1, options_.flush_thread_count);
  for (std::size_t i = 0; i < flush_threads; ++i) {
    flush_threads_.emplace_back([this] { FlushThreadLoop(); });
  }
  stop_apply_ = false;
  apply_thread_ = std::thread([this] { ApplyThreadLoop(); });
  apply_thread_started_ = true;
  stop_compact_ = false;
  const std::size_t compact_threads = std::max<std::size_t>(1, options_.compaction_thread_count);
  for (std::size_t i = 0; i < compact_threads; ++i) {
    compact_threads_.emplace_back([this] { CompactThreadLoop(); });
  }
  return Status::OK();
}

void DB::Impl::StartWalSyncThread() {
  if (options_.wal_sync_interval_ms == 0 || options_.sync_wal) return;
  stop_wal_sync_ = false;
  wal_sync_thread_ = std::thread([this] { WalSyncLoop(); });
}

void DB::Impl::StopWalSyncThread() {
  {
    std::lock_guard lk(wal_sync_mu_);
    stop_wal_sync_ = true;
  }
  wal_sync_cv_.notify_all();
  if (wal_sync_thread_.joinable()) wal_sync_thread_.join();
}

void DB::Impl::WalSyncLoop() {
  const auto interval = std::chrono::milliseconds(options_.wal_sync_interval_ms);
  std::unique_lock lk(wal_sync_mu_);
  while (!stop_wal_sync_) {
    if (wal_sync_cv_.wait_for(lk, interval, [this] { return stop_wal_sync_; })) break;
    lk.unlock();
    wal_->Sync(false);
    metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
    lk.lock();
  }
}

Status DB::Impl::LoadSSTables() {
  std::vector<std::vector<TableRef>> loaded;
  // Try manifest first.
  if (std::filesystem::exists(data_dir_ / "MANIFEST")) {
    Status ms = LoadManifest(loaded);
    if (!ms.ok()) return ms;
  }
  if (!loaded.empty()) {
    std::unique_lock lock(sstable_mu_);
    levels_ = std::move(loaded);
    next_compact_index_.assign(levels_.size(), 0);
    return Status::OK();
  }

  auto ensure_level = [&loaded](std::size_t level) {
    if (loaded.size() <= level) loaded.resize(level + 1);
  };

  auto parse_level = [](const std::string& name, std::size_t& level) -> bool {
    // Expected: sst-l<level>-*.sst
    if (name.rfind("sst-l", 0) != 0) return false;
    auto dash_pos = name.find('-', 5);
    if (dash_pos == std::string::npos) return false;
    auto level_str = name.substr(5, dash_pos - 5);
    try {
      level = static_cast<std::size_t>(std::stoul(level_str));
    } catch (...) {
      return false;
    }
    return true;
  };

  if (std::filesystem::exists(sst_dir_)) {
    for (const auto& entry : std::filesystem::directory_iterator(sst_dir_)) {
      if (!entry.is_regular_file()) continue;
      if (entry.path().extension() != ".sst") continue;
      std::size_t level = 0;
      if (!parse_level(entry.path().filename().string(), level)) continue;
      std::shared_ptr<SSTable> table;
      bool pin_bloom = level > 0;
      Status s = SSTable::Open(entry.path(), block_cache_, bloom_cache_, pin_bloom, table);
      if (!s.ok()) return s;
      ensure_level(level);
      loaded[level].push_back(TableRef{table, table->min_key(), table->max_key(), table->file_size()});
    }
  }

  for (std::size_t i = 0; i < loaded.size(); ++i) {
    auto& level = loaded[i];
    if (i == 0) {
      std::sort(level.begin(), level.end(),
                [](const TableRef& a, const TableRef& b) { return a.table->max_sequence() > b.table->max_sequence(); });
    } else {
      std::sort(level.begin(), level.end(),
                [](const TableRef& a, const TableRef& b) { return a.min_key < b.min_key; });
    }
  }

  std::unique_lock lock(sstable_mu_);
  levels_ = std::move(loaded);
  return WriteManifest();
}

Status DB::Impl::Put(const WriteOptions& options, std::string key, std::string value) {
  std::shared_lock shared(mu_);
  metrics_.puts.fetch_add(1, std::memory_order_relaxed);
  const std::uint64_t seq = next_seq_.fetch_add(1, std::memory_order_relaxed);
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendPut(seq, key, value, sync_now);
  if (!s.ok()) return s;
  s = mem_->Put(seq, key, value);
  if (!s.ok()) return s;
  if (sync_now) metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
  bool need_rotate = mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes;
  shared.unlock();
  if (need_rotate) {
    std::unique_lock lk(mu_);
    if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
      auto imm_mem = std::move(mem_);
      mem_ = std::make_unique<MemTable>(options_.memtable_shard_count, options_.memtable_soft_limit_bytes);
      std::filesystem::path rotated;
      s = RotateWalLocked(seq, rotated);
      if (!s.ok()) return s;
      EnqueueImmutable(std::move(imm_mem), seq, rotated);
    }
  }
  return Status::OK();
}

Status DB::Impl::Delete(const WriteOptions& options, std::string key) {
  std::shared_lock shared(mu_);
  metrics_.deletes.fetch_add(1, std::memory_order_relaxed);
  const std::uint64_t seq = next_seq_.fetch_add(1, std::memory_order_relaxed);
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendDelete(seq, key, sync_now);
  if (!s.ok()) return s;
  s = mem_->Delete(seq, key);
  if (!s.ok()) return s;
  if (sync_now) metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
  bool need_rotate = mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes;
  shared.unlock();
  if (need_rotate) {
    std::unique_lock lk(mu_);
    if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
      auto imm_mem = std::move(mem_);
      mem_ = std::make_unique<MemTable>(options_.memtable_shard_count, options_.memtable_soft_limit_bytes);
      std::filesystem::path rotated;
      s = RotateWalLocked(seq, rotated);
      if (!s.ok()) return s;
      EnqueueImmutable(std::move(imm_mem), seq, rotated);
    }
  }
  return Status::OK();
}

Status DB::Impl::Write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::OK();
  std::shared_lock shared(mu_);
  metrics_.batches.fetch_add(1, std::memory_order_relaxed);
  std::vector<std::uint64_t> seqs;
  seqs.reserve(batch.ops().size());

  for (std::size_t i = 0; i < batch.ops().size(); ++i) {
    seqs.push_back(next_seq_.fetch_add(1, std::memory_order_relaxed));
  }

  for (std::size_t i = 0; i < batch.ops().size(); ++i) {
    const auto& op = batch.ops()[i];
    const auto seq = seqs[i];
    Status s;
    if (op.type == BatchOp::Type::kPut) {
      s = wal_->AppendPut(seq, op.key, op.value, /*sync=*/false);
      if (!s.ok()) return s;
      s = mem_->Put(seq, op.key, op.value);
    } else {
      s = wal_->AppendDelete(seq, op.key, /*sync=*/false);
      if (!s.ok()) return s;
      s = mem_->Delete(seq, op.key);
    }
    if (!s.ok()) return s;
  }

  const bool need_sync = options.sync || options_.sync_wal;
  if (need_sync) {
    metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
    Status s = wal_->Sync(/*force_sync=*/true);
    if (!s.ok()) return s;
  }

  bool need_rotate = mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes;
  shared.unlock();
  if (need_rotate) {
    std::unique_lock lk(mu_);
    if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
      auto imm_mem = std::move(mem_);
      mem_ = std::make_unique<MemTable>(options_.memtable_shard_count, options_.memtable_soft_limit_bytes);
      std::filesystem::path rotated;
      Status s = RotateWalLocked(seqs.back(), rotated);
      if (!s.ok()) return s;
      EnqueueImmutable(std::move(imm_mem), seqs.back(), rotated);
    }
  }
  return Status::OK();
}

Status DB::Impl::Get(const ReadOptions& /*options*/, std::string_view key, std::string& value) {
  metrics_.gets.fetch_add(1, std::memory_order_relaxed);
  // Check memtable first (newest).
  MemEntry entry;
  if (mem_->Get(key, entry)) {
    if (entry.deleted) return Status::NotFound("deleted");
    value = entry.value;
    return Status::OK();
  }

  {
    std::unique_lock lk(flush_mu_);
    for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
      if ((*it)->mem && (*it)->mem->Get(key, entry)) {
        if (entry.deleted) return Status::NotFound("deleted");
        value = entry.value;
        return Status::OK();
      }
    }
  }

  std::shared_lock lock(sstable_mu_);
  if (!levels_.empty()) {
    // Level 0: check newest first, may overlap.
    for (const auto& t : levels_[0]) {
      if (t.table->Get(key, entry)) {
        if (entry.deleted) return Status::NotFound("deleted");
        value = entry.value;
        return Status::OK();
      }
    }
    for (std::size_t lvl = 1; lvl < levels_.size(); ++lvl) {
      const auto& level = levels_[lvl];
      auto it = std::lower_bound(
          level.begin(), level.end(), key,
          [](const TableRef& t, std::string_view k) { return t.max_key < k; });
      if (it != level.end() && it->min_key <= key && key <= it->max_key) {
        if (it->table->Get(key, entry)) {
          if (entry.deleted) return Status::NotFound("deleted");
          value = entry.value;
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("missing");
}

Status DB::Impl::BuildMergedView(std::map<std::string, MemEntry>& merged, std::uint64_t snapshot_seq) {
  std::vector<MemEntry> mem_entries;
  std::vector<std::shared_ptr<ImmutableMem>> imm_copy;
  {
    std::shared_lock mu_lk(mu_);
    mem_entries = mem_->Snapshot();
    std::lock_guard lk(flush_mu_);
    imm_copy.assign(immutables_.begin(), immutables_.end());
  }
  for (auto& e : mem_entries) {
    if (e.seq > snapshot_seq) continue;
    auto it = merged.find(e.key);
    if (it == merged.end() || it->second.seq < e.seq) {
      merged[e.key] = std::move(e);
    }
  }

  for (const auto& imm : imm_copy) {
    if (!imm || !imm->mem) continue;
    auto imm_entries = imm->mem->Snapshot();
    for (auto& e : imm_entries) {
      if (e.seq > snapshot_seq) continue;
      auto it = merged.find(e.key);
      if (it == merged.end() || it->second.seq < e.seq) {
        merged[e.key] = std::move(e);
      }
    }
  }

  std::shared_lock lock(sstable_mu_);
  for (const auto& level : levels_) {
    for (const auto& tref : level) {
      std::vector<MemEntry> entries;
      Status s = tref.table->LoadAll(entries);
      if (!s.ok()) return s;
      for (auto& e : entries) {
        if (e.seq > snapshot_seq) continue;
        auto it = merged.find(e.key);
        if (it == merged.end() || it->second.seq < e.seq) {
          merged[e.key] = std::move(e);
        }
      }
    }
  }
  return Status::OK();
}

std::uint64_t DB::Impl::SnapshotSeq(const ReadOptions& options) const {
  if (options.snapshot_seq != 0) return options.snapshot_seq;
  if (options.snapshot) {
    auto seq = next_seq_.load(std::memory_order_relaxed);
    return seq ? seq - 1 : 0;
  }
  return UINT64_MAX;
}

std::uint64_t DB::Impl::RegisterSnapshot(std::uint64_t seq) {
  std::lock_guard lk(snapshot_mu_);
  snapshots_.insert(seq);
  return seq;
}

void DB::Impl::UnregisterSnapshot(std::uint64_t seq) {
  std::lock_guard lk(snapshot_mu_);
  auto it = snapshots_.find(seq);
  if (it != snapshots_.end()) snapshots_.erase(it);
}

std::uint64_t DB::Impl::MinActiveSnapshot() const {
  std::lock_guard lk(snapshot_mu_);
  if (snapshots_.empty()) return UINT64_MAX;
  return *snapshots_.begin();
}

std::unique_ptr<DB::Iterator> DB::Impl::NewIterator(const ReadOptions& options, std::string_view prefix) {
  std::uint64_t snap = SnapshotSeq(options);
  std::map<std::string, MemEntry> merged;
  Status s = BuildMergedView(merged, snap);
  auto rep = std::make_unique<DB::Iterator::Rep>();
  rep->prefix.assign(prefix);
  rep->snapshot_seq = snap;
  if (!s.ok()) {
    rep->status = s;
    return std::unique_ptr<DB::Iterator>(new DB::Iterator(std::move(rep)));
  }

  rep->data.reserve(merged.size());
  for (auto& kv : merged) {
    if (kv.second.deleted) continue;
    if (!rep->prefix.empty() && kv.first.rfind(rep->prefix, 0) != 0) continue;
    rep->data.emplace_back(std::move(kv.first), std::move(kv.second.value));
  }
  rep->pos = rep->data.empty() ? rep->data.size() : 0;  // pos==size means invalid; use 0 if non-empty
  if (options.snapshot || options.snapshot_seq != 0) {
    rep->registered = true;
    rep->owner = this;
    rep->snapshot_seq = RegisterSnapshot(snap);
  } else {
    rep->registered = false;
    rep->owner = this;
  }
  return std::unique_ptr<DB::Iterator>(new DB::Iterator(std::move(rep)));
}

Status DB::Impl::Flush() {
  std::unique_lock lock(mu_);
  Status s = FlushLocked();
  lock.unlock();
  if (!s.ok()) return s;
  MaybeScheduleCompaction();
  return WaitForAllCompactions();
}

Status DB::Impl::FlushLocked() {
  if (mem_->Empty() && immutables_.empty()) return Status::OK();
  if (!mem_->Empty()) {
    auto imm_mem = std::move(mem_);
    mem_ = std::make_unique<MemTable>(options_.memtable_shard_count, options_.memtable_soft_limit_bytes);
    std::filesystem::path rotated;
    auto cur = next_seq_.load(std::memory_order_relaxed);
    std::uint64_t max_seq = cur ? cur - 1 : 0;
    Status s = RotateWalLocked(max_seq, rotated);
    if (!s.ok()) return s;
    EnqueueImmutable(std::move(imm_mem), max_seq, rotated);
  }
  std::unique_lock lk(flush_mu_);
  flush_cv_.wait(lk, [this] {
    return immutables_.empty() && in_flight_flush_ == 0 && pending_apply_.load(std::memory_order_relaxed) == 0;
  });
  return Status::OK();
}

Status DB::Impl::WriteManifest() {
  std::lock_guard<std::mutex> mlk(manifest_mu_);
  std::vector<std::tuple<std::size_t, std::string, std::string, std::string, std::uint64_t, std::uint64_t>> entries;
  for (std::size_t lvl = 0; lvl < levels_.size(); ++lvl) {
    for (const auto& t : levels_[lvl]) {
      auto rel = t.table->path().lexically_relative(data_dir_).string();
      if (rel.empty() || rel[0] == '.') rel = t.table->path().filename().string();
      entries.emplace_back(lvl, rel, t.min_key, t.max_key, t.size, t.table->max_sequence());
    }
  }

  const auto manifest_tmp =
      data_dir_ / ("MANIFEST.tmp." + std::to_string(manifest_tmp_seq_.fetch_add(1, std::memory_order_relaxed)));
  const auto manifest = data_dir_ / "MANIFEST";
  {
    std::ofstream out(manifest_tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return Status::IOError("open MANIFEST.tmp failed");
    out << "MANIFEST 1\n";
    for (const auto& e : entries) {
      out << std::get<0>(e) << "|" << std::get<1>(e) << "|" << std::get<2>(e) << "|" << std::get<3>(e) << "|"
          << std::get<4>(e) << "|" << std::get<5>(e) << "\n";
    }
    out.flush();
    if (!out) return Status::IOError("write MANIFEST.tmp failed");
  }
  Status s = SyncFileToDisk(manifest_tmp);
  if (!s.ok()) return s;
  s = SyncParentDir(manifest_tmp);
  if (!s.ok()) return s;
  std::error_code ec;
  std::filesystem::rename(manifest_tmp, manifest, ec);
  if (ec) return Status::IOError("rename MANIFEST.tmp failed: " + ec.message());
  s = SyncParentDir(manifest);
  if (!s.ok()) return s;
  return Status::OK();
}

Status DB::Impl::LoadManifest(std::vector<std::vector<TableRef>>& loaded) {
  const auto manifest = data_dir_ / "MANIFEST";
  std::ifstream in(manifest);
  if (!in.is_open()) return Status::IOError("failed to open MANIFEST");
  std::string header;
  if (!std::getline(in, header)) return Status::Corruption("empty MANIFEST");
  if (header != "MANIFEST 1") return Status::Corruption("unsupported MANIFEST version");

  auto ensure_level = [&loaded](std::size_t level) {
    if (loaded.size() <= level) loaded.resize(level + 1);
  };

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::stringstream ss(line);
    std::string lvl_str, path_str, min_key, max_key, size_str, max_seq_str;
    if (!std::getline(ss, lvl_str, '|')) break;
    if (!std::getline(ss, path_str, '|')) break;
    if (!std::getline(ss, min_key, '|')) break;
    if (!std::getline(ss, max_key, '|')) break;
    if (!std::getline(ss, size_str, '|')) break;
    if (!std::getline(ss, max_seq_str, '|')) break;
    std::size_t level = 0;
    std::uint64_t size = 0;
    try {
      level = static_cast<std::size_t>(std::stoul(lvl_str));
      size = static_cast<std::uint64_t>(std::stoull(size_str));
      (void)std::stoull(max_seq_str);
    } catch (...) {
      return Status::Corruption("bad MANIFEST line");
    }
    auto path = data_dir_ / path_str;
    std::shared_ptr<SSTable> table;
    bool pin_bloom = level > 0;
    Status s = SSTable::Open(path, block_cache_, bloom_cache_, pin_bloom, table);
    if (!s.ok()) return s;
    ensure_level(level);
    loaded[level].push_back(TableRef{table, min_key, max_key, size == 0 ? table->file_size() : size});
  }
  return Status::OK();
}

std::uint64_t DB::Impl::LevelMaxBytes(std::size_t level) const {
  if (level == 0) {
    return static_cast<std::uint64_t>(options_.level0_file_limit * options_.sstable_target_size_bytes);
  }
  double base = static_cast<double>(options_.level_base_bytes);
  double mul = 1.0;
  for (std::size_t i = 1; i < level; ++i) mul *= static_cast<double>(options_.level_size_multiplier);
  return static_cast<std::uint64_t>(base * mul);
}

std::uint64_t DB::Impl::LevelBytes(std::size_t level) const {
  if (level >= levels_.size()) return 0;
  std::uint64_t total = 0;
  for (const auto& t : levels_[level]) {
    total += t.size;
  }
  return total;
}

bool DB::Impl::PickCompactionLevel(std::size_t& target_level) {
  std::shared_lock lock(sstable_mu_);
  if (!levels_.empty() && levels_[0].size() > options_.level0_file_limit) {
    target_level = 0;
    return true;
  }
  for (std::size_t lvl = 1; lvl < levels_.size(); ++lvl) {
    if (LevelBytes(lvl) > LevelMaxBytes(lvl)) {
      target_level = lvl;
      return true;
    }
  }
  return false;
}

void DB::Impl::EnqueueCompaction(std::size_t level) {
  std::lock_guard lk(compact_mu_);
  if (stop_compact_) return;
  if (compact_scheduled_.size() <= level) compact_scheduled_.resize(level + 1, false);
  if (compact_scheduled_[level]) return;
  compact_queue_.push_back(level);
  compact_scheduled_[level] = true;
  compact_cv_.notify_one();
}

void DB::Impl::MaybeScheduleCompaction() {
  std::size_t level = 0;
  if (PickCompactionLevel(level)) {
    EnqueueCompaction(level);
  }
}

Status DB::Impl::WaitForAllCompactions() {
  std::unique_lock lk(compact_mu_);
  compact_cv_.wait(lk, [this] {
    if (!compact_queue_.empty() || compact_inflight_ > 0) return false;
    std::size_t lvl = 0;
    return !PickCompactionLevel(lvl);
  });
  std::lock_guard status_lk(compact_status_mu_);
  return last_compact_status_;
}

Status DB::Impl::RotateWalLocked(std::uint64_t max_seq_for_old, std::filesystem::path& rotated_path) {
  // Assume mu_ is held.
  std::lock_guard guard(wal_sync_mu_);
  wal_->Close();
  rotated_path = data_dir_ / ("wal-" + std::to_string(max_seq_for_old) + ".log");
  std::error_code ec;
  std::filesystem::rename(wal_path_, rotated_path, ec);
  if (ec) return Status::IOError("failed to rotate wal: " + ec.message());
  wal_ = std::make_unique<WAL>(wal_path_, options_.sync_wal, options_.enable_crc);
  return wal_->Open();
}

void DB::Impl::EnqueueImmutable(std::unique_ptr<MemTable> mem, std::uint64_t max_seq,
                                std::filesystem::path wal_path) {
  {
    std::unique_lock lk(flush_mu_);
    flush_cv_.wait(lk, [this] {
      return stop_flush_ ||
             (immutables_.size() + in_flight_flush_ < options_.max_immutable_memtables);
    });
    if (stop_flush_) return;
    immutables_.push_back(std::make_shared<ImmutableMem>(ImmutableMem{
        std::move(mem), std::move(wal_path), max_seq}));
  }
  flush_cv_.notify_all();
}

void DB::Impl::MaybeScheduleFlush() {
  // No-op: flush thread waits on condition variable; notify is done in EnqueueImmutable.
}

Status DB::Impl::FlushImmutable(const std::shared_ptr<ImmutableMem>& imm) {
  auto start = std::chrono::steady_clock::now();
  auto entries_view = imm->mem->SnapshotViews();
  if (entries_view.empty()) {
    {
      std::lock_guard lk(apply_mu_);
      apply_queue_.push_back(FlushResult{{}, imm->wal_path, 0, 0, imm});
      pending_apply_.fetch_add(1, std::memory_order_relaxed);
    }
    apply_cv_.notify_one();
    return Status::OK();
  }

  std::vector<TableRef> new_tables;
  std::uint64_t total_bytes = 0;
  std::vector<MemEntryView> chunk;
  std::uint64_t chunk_approx = 0;
  auto flush_chunk = [&](std::vector<MemEntryView>& data) -> Status {
    if (data.empty()) return Status::OK();
    std::uint64_t chunk_max_seq = 0;
    for (const auto& e : data) chunk_max_seq = std::max(chunk_max_seq, e.seq);
    const auto filename = "sst-l0-" + std::to_string(chunk_max_seq) + "-" + std::to_string(new_tables.size()) + ".sst";
    const auto path = sst_dir_ / filename;

    Status ws = SSTable::Write(path, data, options_.sstable_block_size_bytes, options_.bloom_bits_per_key,
                               options_.enable_compress);
    if (!ws.ok()) return ws;

    std::shared_ptr<SSTable> table;
    ws = SSTable::Open(path, block_cache_, bloom_cache_, false, table);
    if (!ws.ok()) return ws;

    new_tables.push_back(TableRef{table, table->min_key(), table->max_key(), table->file_size()});
    total_bytes += table->file_size();
    data.clear();
    chunk_approx = 0;
    return Status::OK();
  };

  for (const auto& e : entries_view) {
    const std::uint64_t entry_sz = 1 + 8 + 4 + 4 + e.key.size() + e.value.size();
    chunk.push_back(e);
    chunk_approx += entry_sz;
    if (chunk_approx >= options_.sstable_target_size_bytes) {
      Status ws = flush_chunk(chunk);
      if (!ws.ok()) return ws;
    }
  }
  Status s = flush_chunk(chunk);
  if (!s.ok()) return s;

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
                     .count();
  {
    std::lock_guard lk(apply_mu_);
    apply_queue_.push_back(FlushResult{std::move(new_tables), imm->wal_path, total_bytes,
                                       static_cast<std::uint64_t>(elapsed), imm});
    pending_apply_.fetch_add(1, std::memory_order_relaxed);
  }
  apply_cv_.notify_one();
  return Status::OK();
}

void DB::Impl::FlushThreadLoop() {
  while (true) {
    std::shared_ptr<ImmutableMem> imm;
    {
      std::unique_lock lk(flush_mu_);
      auto has_pending = [this]() {
        for (const auto& m : immutables_) {
          if (m && !m->flushing && !m->flushed) return true;
        }
        return false;
      };
      flush_cv_.wait(lk, [this, &has_pending] { return stop_flush_ || has_pending(); });
      if (stop_flush_ && !has_pending()) break;
      for (auto& cand : immutables_) {
        if (cand && !cand->flushing && !cand->flushed) {
          imm = cand;
          cand->flushing = true;
          ++in_flight_flush_;
          break;
        }
      }
    }
    if (!imm) continue;
    Status s = FlushImmutable(imm);
    if (!s.ok()) {
      // Best-effort log.
      std::cerr << "FlushImmutable failed: " << s.ToString() << "\n";
    }
    {
      std::lock_guard lk(flush_mu_);
      imm->flushing = false;
      if (s.ok()) imm->flushed = true;
      if (in_flight_flush_ > 0) --in_flight_flush_;
      flush_cv_.notify_all();
    }
  }
}

void DB::Impl::ApplyThreadLoop() {
  while (true) {
    FlushResult res;
    {
      std::unique_lock lk(apply_mu_);
      apply_cv_.wait(lk, [this] { return stop_apply_ || !apply_queue_.empty(); });
      if (stop_apply_ && apply_queue_.empty()) break;
      res = std::move(apply_queue_.front());
      apply_queue_.pop_front();
    }

    if (!res.tables.empty()) {
      {
        std::unique_lock lock_tables(sstable_mu_);
        if (levels_.empty()) levels_.resize(1);
        if (next_compact_index_.size() < levels_.size()) next_compact_index_.resize(levels_.size(), 0);
        for (auto& t : res.tables) {
          levels_[0].insert(levels_[0].begin(), std::move(t));
        }
        std::sort(levels_[0].begin(), levels_[0].end(),
                  [](const TableRef& a, const TableRef& b) { return a.table->max_sequence() > b.table->max_sequence(); });
      }
      Status ms = WriteManifest();
      if (!ms.ok()) {
        std::cerr << "WriteManifest failed: " << ms.ToString() << "\n";
      } else {
        metrics_.flushes.fetch_add(1, std::memory_order_relaxed);
        metrics_.flush_ms.fetch_add(res.flush_ms, std::memory_order_relaxed);
        metrics_.flush_bytes.fetch_add(res.total_bytes, std::memory_order_relaxed);
      }
    }
    std::error_code ec;
    std::filesystem::remove(res.wal_path, ec);

    if (res.imm) {
      std::lock_guard lk(flush_mu_);
      res.imm->applied = true;
      res.imm->mem.reset();
      res.imm->wal_path.clear();
      immutables_.erase(std::remove_if(immutables_.begin(), immutables_.end(),
                                       [&](const std::shared_ptr<ImmutableMem>& ptr) {
                                         return ptr && ptr->applied;
                                       }),
                        immutables_.end());
      flush_cv_.notify_all();
    }

    pending_apply_.fetch_sub(1, std::memory_order_relaxed);
    flush_cv_.notify_all();

    MaybeScheduleCompaction();
  }
}

void DB::Impl::CompactThreadLoop() {
  while (true) {
    std::size_t level = 0;
    {
      std::unique_lock lk(compact_mu_);
      compact_cv_.wait(lk, [this] { return stop_compact_ || !compact_queue_.empty(); });
      if (stop_compact_ && compact_queue_.empty()) break;
      level = compact_queue_.front();
      compact_queue_.pop_front();
      ++compact_inflight_;
    }

    Status s = CompactLevel(level);
    {
      std::lock_guard status_lk(compact_status_mu_);
      last_compact_status_ = s;
    }
    if (!s.ok()) {
      std::cerr << "CompactLevel(" << level << ") failed: " << s.ToString() << "\n";
    }

    {
      std::lock_guard lk(compact_mu_);
      if (level < compact_scheduled_.size()) compact_scheduled_[level] = false;
      if (compact_inflight_ > 0) --compact_inflight_;
      compact_cv_.notify_all();
    }
    MaybeScheduleCompaction();
  }
}

static bool Overlaps(const std::string& a_min, const std::string& a_max, const std::string& b_min,
                     const std::string& b_max) {
  return !(a_max < b_min || b_max < a_min);
}

Status DB::Impl::CompactLevel(std::size_t level) {
  auto start = std::chrono::steady_clock::now();
  std::vector<TableRef> inputs;
  std::vector<TableRef> next_inputs;
  {
    std::unique_lock lock(sstable_mu_);
    if (level >= levels_.size()) return Status::OK();
    if (levels_[level].empty()) return Status::OK();
    if (level == 0) {
      inputs = levels_[level];
      if (level + 1 < levels_.size()) {
        for (const auto& t : levels_[level + 1]) {
          for (const auto& in : inputs) {
            if (Overlaps(t.min_key, t.max_key, in.min_key, in.max_key)) {
              next_inputs.push_back(t);
              break;
            }
          }
        }
      }
    } else {
      if (next_compact_index_.size() <= level) next_compact_index_.resize(level + 1, 0);
      std::size_t idx = next_compact_index_[level] % levels_[level].size();
      inputs.push_back(levels_[level][idx]);
      if (level + 1 < levels_.size()) {
        for (const auto& t : levels_[level + 1]) {
          if (Overlaps(t.min_key, t.max_key, inputs[0].min_key, inputs[0].max_key)) {
            next_inputs.push_back(t);
          }
        }
      }
      next_compact_index_[level] = (idx + 1) % levels_[level].size();
    }
  }

  const std::uint64_t keep_seq = MinActiveSnapshot();
  struct Cursor {
    std::shared_ptr<SSTable> table;
    std::size_t block_idx{0};
    std::size_t entry_idx{0};
    std::vector<MemEntry> block;
  };

  auto load_block = [&](Cursor& cur) -> Status {
    if (cur.block_idx >= cur.table->block_count()) {
      return Status::NotFound("end");
    }
    Status bs = cur.table->ReadBlockByIndex(cur.block_idx, cur.block);
    if (!bs.ok()) return bs;
    ++cur.block_idx;
    cur.entry_idx = 0;
    return Status::OK();
  };

  auto next_entry = [&](Cursor& cur, MemEntry& out) -> Status {
    while (true) {
      if (cur.entry_idx < cur.block.size()) {
        out = cur.block[cur.entry_idx++];
        return Status::OK();
      }
      Status ls = load_block(cur);
      if (ls.code() == Status::Code::kNotFound) return ls;
      if (!ls.ok()) return ls;
    }
  };

  std::vector<Cursor> cursors;
  cursors.reserve(inputs.size() + next_inputs.size());
  struct HeapItem {
    std::string key;
    MemEntry entry;
    std::size_t cursor_idx{0};
  };
  auto cmp = [](const HeapItem& a, const HeapItem& b) { return a.key > b.key; };
  std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmp)> heap(cmp);

  auto add_cursor = [&](const TableRef& tref) -> Status {
    Cursor cur;
    cur.table = tref.table;
    MemEntry first;
    Status ns = next_entry(cur, first);
    if (ns.code() == Status::Code::kNotFound) return Status::OK();
    if (!ns.ok()) return ns;
    const std::size_t idx = cursors.size();
    cursors.push_back(std::move(cur));
    heap.push(HeapItem{first.key, std::move(first), idx});
    return Status::OK();
  };

  for (const auto& t : inputs) {
    Status ns = add_cursor(t);
    if (!ns.ok()) return ns;
  }
  for (const auto& t : next_inputs) {
    Status ns = add_cursor(t);
    if (!ns.ok()) return ns;
  }

  if (heap.empty()) return Status::OK();

  std::vector<TableRef> outputs;
  std::vector<MemEntry> chunk;
  std::uint64_t approx_size = 0;
  auto flush_chunk = [&](std::vector<MemEntry>& data) -> Status {
    if (data.empty()) return Status::OK();
    std::uint64_t max_seq = 0;
    for (const auto& e : data) max_seq = std::max(max_seq, e.seq);
    const auto filename =
        "sst-l" + std::to_string(level + 1) + "-" + std::to_string(max_seq) + "-" +
        std::to_string(outputs.size()) + ".sst";
    const auto path = sst_dir_ / filename;
    Status ws = SSTable::Write(path, data, options_.sstable_block_size_bytes,
                               options_.bloom_bits_per_key, options_.enable_compress);
    if (!ws.ok()) return ws;
    std::shared_ptr<SSTable> t;
    bool pin_bloom = (level + 1) > 0;
    ws = SSTable::Open(path, block_cache_, bloom_cache_, pin_bloom, t);
    if (!ws.ok()) return ws;
    outputs.push_back(TableRef{t, t->min_key(), t->max_key(), t->file_size()});
    data.clear();
    approx_size = 0;
    return Status::OK();
  };

  auto add_entry = [&](MemEntry&& e) -> Status {
    std::uint64_t entry_sz = 1 + 8 + 4 + 4 + e.key.size() + e.value.size();
    chunk.push_back(std::move(e));
    approx_size += entry_sz;
    if (approx_size >= options_.sstable_target_size_bytes) {
      return flush_chunk(chunk);
    }
    return Status::OK();
  };

  while (!heap.empty()) {
    const std::string current_key = heap.top().key;
    std::vector<MemEntry> same_key_entries;
    while (!heap.empty() && heap.top().key == current_key) {
      HeapItem item = heap.top();
      heap.pop();
      same_key_entries.push_back(std::move(item.entry));
      MemEntry next;
      Status ns = next_entry(cursors[item.cursor_idx], next);
      if (ns.ok()) {
        heap.push(HeapItem{next.key, std::move(next), item.cursor_idx});
      } else if (ns.code() != Status::Code::kNotFound) {
        return ns;
      }
    }

    if (same_key_entries.empty()) continue;
    std::size_t newest_idx = 0;
    std::size_t snapshot_idx = same_key_entries.size();
    for (std::size_t i = 0; i < same_key_entries.size(); ++i) {
      if (same_key_entries[i].seq > same_key_entries[newest_idx].seq) {
        newest_idx = i;
      }
      if (keep_seq != UINT64_MAX && same_key_entries[i].seq <= keep_seq) {
        if (snapshot_idx == same_key_entries.size() ||
            same_key_entries[i].seq > same_key_entries[snapshot_idx].seq) {
          snapshot_idx = i;
        }
      }
    }

    Status add_newest = add_entry(std::move(same_key_entries[newest_idx]));
    if (!add_newest.ok()) return add_newest;

    if (keep_seq != UINT64_MAX && same_key_entries[newest_idx].seq > keep_seq &&
        snapshot_idx < same_key_entries.size()) {
      Status add_snapshot = add_entry(std::move(same_key_entries[snapshot_idx]));
      if (!add_snapshot.ok()) return add_snapshot;
    }
  }

  Status s = flush_chunk(chunk);
  if (!s.ok()) return s;

  std::uint64_t input_bytes = 0;
  for (const auto& t : inputs) input_bytes += t.size;
  for (const auto& t : next_inputs) input_bytes += t.size;
  std::uint64_t output_bytes = 0;

  std::unique_lock lock(sstable_mu_);
  if (levels_.size() <= level + 1) levels_.resize(level + 2);
  if (next_compact_index_.size() < levels_.size()) next_compact_index_.resize(levels_.size(), 0);
  // Remove old files from level and next.
  auto remove_tables = [](std::vector<TableRef>& vec, const std::vector<TableRef>& to_remove) {
    vec.erase(std::remove_if(vec.begin(), vec.end(),
                             [&](const TableRef& t) {
                               return std::any_of(to_remove.begin(), to_remove.end(),
                                                  [&](const TableRef& r) { return t.table->path() == r.table->path(); });
                             }),
              vec.end());
  };
  remove_tables(levels_[level], inputs);
  if (level + 1 < levels_.size()) {
    remove_tables(levels_[level + 1], next_inputs);
  }
  // Add outputs to next level.
  for (auto& t : outputs) {
    levels_[level + 1].push_back(std::move(t));
    output_bytes += levels_[level + 1].back().size;
  }
  // If we removed a single file from level>0, adjust next_compact_index_ bounds.
  if (level > 0) {
    if (next_compact_index_[level] >= levels_[level].size()) next_compact_index_[level] = 0;
  }
  if (level + 1 > 0) {
    std::sort(levels_[level + 1].begin(), levels_[level + 1].end(),
              [](const TableRef& a, const TableRef& b) { return a.min_key < b.min_key; });
  }

  // Delete old files on disk.
  for (const auto& t : inputs) {
    std::error_code ec;
    std::filesystem::remove(t.table->path(), ec);
  }
  for (const auto& t : next_inputs) {
    std::error_code ec;
    std::filesystem::remove(t.table->path(), ec);
  }
  Status ms = WriteManifest();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
                     .count();
  metrics_.compactions.fetch_add(1, std::memory_order_relaxed);
  metrics_.compaction_ms.fetch_add(static_cast<std::uint64_t>(elapsed), std::memory_order_relaxed);
  metrics_.compaction_input_bytes.fetch_add(input_bytes, std::memory_order_relaxed);
  metrics_.compaction_output_bytes.fetch_add(output_bytes, std::memory_order_relaxed);
  return ms;
}

Status DB::Impl::Compact() {
  std::unique_lock lock(mu_);
  Status s = FlushLocked();
  lock.unlock();
  if (!s.ok()) return s;
  MaybeScheduleCompaction();
  return WaitForAllCompactions();
}

Metrics DB::Impl::GetMetrics() const {
  Metrics m;
  m.puts = metrics_.puts.load(std::memory_order_relaxed);
  m.deletes = metrics_.deletes.load(std::memory_order_relaxed);
  m.gets = metrics_.gets.load(std::memory_order_relaxed);
  m.batches = metrics_.batches.load(std::memory_order_relaxed);
  m.flushes = metrics_.flushes.load(std::memory_order_relaxed);
  m.flush_ms = metrics_.flush_ms.load(std::memory_order_relaxed);
  m.flush_bytes = metrics_.flush_bytes.load(std::memory_order_relaxed);
  m.compactions = metrics_.compactions.load(std::memory_order_relaxed);
  m.compaction_ms = metrics_.compaction_ms.load(std::memory_order_relaxed);
  m.compaction_input_bytes = metrics_.compaction_input_bytes.load(std::memory_order_relaxed);
  m.compaction_output_bytes = metrics_.compaction_output_bytes.load(std::memory_order_relaxed);
  m.wal_syncs = metrics_.wal_syncs.load(std::memory_order_relaxed);
  if (block_cache_) {
    auto stats = block_cache_->GetStats();
    m.block_cache_hits = stats.hits;
    m.block_cache_misses = stats.misses;
    m.block_cache_puts = stats.puts;
    m.block_cache_evictions = stats.evictions;
    m.block_cache_used_bytes = stats.used_bytes;
    m.block_cache_capacity_bytes = stats.capacity_bytes;
  }
  return m;
}

DB::DB(Options options) : impl_(new Impl(std::move(options))) {}
DB::~DB() = default;

  DB::Impl::~Impl() {
    {
      std::lock_guard lk(flush_mu_);
      stop_flush_ = true;
    }
    flush_cv_.notify_all();
    for (auto& th : flush_threads_) {
      if (th.joinable()) th.join();
    }
    {
      std::lock_guard lk(apply_mu_);
      stop_apply_ = true;
    }
    apply_cv_.notify_all();
    if (apply_thread_started_ && apply_thread_.joinable()) apply_thread_.join();
    {
      std::lock_guard lk(compact_mu_);
      stop_compact_ = true;
    }
    compact_cv_.notify_all();
    for (auto& th : compact_threads_) {
      if (th.joinable()) th.join();
    }
    StopWalSyncThread();
  }

Status DB::Open(const Options& options, std::unique_ptr<DB>& db) {
  auto impl = std::make_unique<Impl>(options);
  Status s = impl->Init();
  if (!s.ok()) return s;
  db.reset(new DB(options));
  db->impl_ = std::move(impl);
  return Status::OK();
}

Status DB::Put(const WriteOptions& options, std::string key, std::string value) {
  return impl_->Put(options, std::move(key), std::move(value));
}

Status DB::Delete(const WriteOptions& options, std::string key) {
  return impl_->Delete(options, std::move(key));
}

Status DB::Write(const WriteOptions& options, const WriteBatch& batch) {
  return impl_->Write(options, batch);
}

Status DB::Get(const ReadOptions& options, std::string_view key, std::string& value) {
  return impl_->Get(options, key, value);
}

Status DB::Flush() { return impl_->Flush(); }

Status DB::Compact() { return impl_->Compact(); }

std::unique_ptr<DB::Iterator> DB::Scan(const ReadOptions& options, std::string_view prefix) {
  return impl_->NewIterator(options, prefix);
}

Metrics DB::GetMetrics() const { return impl_->GetMetrics(); }

DB::Iterator::Iterator(std::unique_ptr<Rep> rep) : rep_(std::move(rep)) {}
DB::Iterator::~Iterator() {
  if (rep_ && rep_->registered && rep_->owner) {
    rep_->owner->UnregisterSnapshot(rep_->snapshot_seq);
    rep_->registered = false;
  }
}
DB::Iterator::Iterator(Iterator&& other) noexcept = default;
DB::Iterator& DB::Iterator::operator=(Iterator&& other) noexcept = default;

void DB::Iterator::SeekToFirst() {
  if (!rep_ || !rep_->status.ok()) return;
  rep_->pos = rep_->data.empty() ? rep_->data.size() : 0;
}

void DB::Iterator::Seek(std::string_view target) {
  if (!rep_ || !rep_->status.ok()) return;
  auto it = std::lower_bound(
      rep_->data.begin(), rep_->data.end(), target,
      [](const std::pair<std::string, std::string>& kv, std::string_view t) { return kv.first < t; });
  rep_->pos = static_cast<std::size_t>(std::distance(rep_->data.begin(), it));
  if (!rep_->prefix.empty() && Valid() && rep_->data[rep_->pos].first.rfind(rep_->prefix, 0) != 0) {
    rep_->pos = rep_->data.size();
  }
}

bool DB::Iterator::Valid() const {
  return rep_ && rep_->status.ok() && rep_->pos < rep_->data.size();
}

void DB::Iterator::Next() {
  if (Valid()) ++rep_->pos;
  if (!rep_->prefix.empty()) {
    while (Valid() && rep_->data[rep_->pos].first.rfind(rep_->prefix, 0) != 0) {
      rep_->pos = rep_->data.size();
    }
  }
}

std::string_view DB::Iterator::key() const {
  if (!Valid()) return {};
  return rep_->data[rep_->pos].first;
}

std::string_view DB::Iterator::value() const {
  if (!Valid()) return {};
  return rep_->data[rep_->pos].second;
}

Status DB::Iterator::status() const {
  if (!rep_) return Status::IOError("iterator not initialized");
  return rep_->status;
}

std::string_view DB::Iterator::prefix() const {
  if (!rep_) return {};
  return rep_->prefix;
}

}  // namespace dkv
