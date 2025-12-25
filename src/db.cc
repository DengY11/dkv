#include "dkv/db.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <atomic>

#include "block_cache.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"

namespace dkv {

class DB::Impl {
 public:
  explicit Impl(Options options)
      : options_(std::move(options)),
        data_dir_(options_.data_dir),
        wal_path_(data_dir_ / "wal.log"),
        sst_dir_(data_dir_ / "sst"),
        mem_(std::make_unique<MemTable>()) {}
  ~Impl();

  Status Init();

  Status Put(const WriteOptions& options, std::string key, std::string value);
  Status Delete(const WriteOptions& options, std::string key);
  Status Write(const WriteOptions& options, const WriteBatch& batch);
  Status Get(const ReadOptions& options, std::string_view key, std::string& value);
  Status Flush();
  Status Compact();
  Status Scan(const ReadOptions& options, std::string_view from, std::size_t limit,
              std::vector<std::pair<std::string, std::string>>& out);
  Metrics GetMetrics() const;

 private:
  struct TableRef;
  struct ImmutableMem;
  void StartWalSyncThread();
  void StopWalSyncThread();
  void WalSyncLoop();
  Status FlushLocked();
  Status LoadSSTables();
  Status LoadManifest(std::vector<std::vector<TableRef>>& loaded);
  Status WriteManifest();
  Status MaybeCompactLocked();
  Status CompactLevel(std::size_t level);
  std::uint64_t LevelMaxBytes(std::size_t level) const;
  std::uint64_t LevelBytes(std::size_t level) const;
  Status RotateWalLocked(std::uint64_t max_seq_for_old, std::filesystem::path& rotated_path);
  void MaybeScheduleFlush();
  void FlushThreadLoop();
  void EnqueueImmutable(std::unique_ptr<MemTable> mem, std::uint64_t max_seq, std::filesystem::path wal_path);
  Status FlushImmutable(ImmutableMem&& imm);

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
  };
  std::vector<ImmutableMem> immutables_;
  std::thread flush_thread_;
  std::condition_variable flush_cv_;
  std::mutex flush_mu_;
  bool stop_flush_{false};
  std::shared_ptr<BlockCache> block_cache_;

  std::mutex mu_;  // guards mem_, wal_ operations, seq_ and flush/compaction exclusivity.
  std::uint64_t next_seq_{1};

  std::thread wal_sync_thread_;
  std::condition_variable wal_sync_cv_;
  std::mutex wal_sync_mu_;
  bool stop_wal_sync_{false};

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
};

Status DB::Impl::Init() {
  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);

  wal_ = std::make_unique<WAL>(wal_path_, options_.sync_wal, options_.enable_crc);
  Status s = wal_->Open();
  if (!s.ok()) return s;

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

  next_seq_ = max_seq_seen + 1;
  if (options_.block_cache_capacity_bytes > 0) {
    block_cache_ = std::make_shared<BlockCache>(options_.block_cache_capacity_bytes);
  }
  StartWalSyncThread();
  stop_flush_ = false;
  flush_thread_ = std::thread([this] { FlushThreadLoop(); });
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
      Status s = SSTable::Open(entry.path(), block_cache_, table);
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
  std::lock_guard lock(mu_);
  metrics_.puts.fetch_add(1, std::memory_order_relaxed);
  const std::uint64_t seq = next_seq_++;
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendPut(seq, key, value, sync_now);
  if (!s.ok()) return s;
  s = mem_->Put(seq, key, value);
  if (!s.ok()) return s;
  if (sync_now) metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
  if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    auto imm_mem = std::move(mem_);
    mem_ = std::make_unique<MemTable>();
    std::filesystem::path rotated;
    s = RotateWalLocked(seq, rotated);
    if (!s.ok()) return s;
    EnqueueImmutable(std::move(imm_mem), seq, rotated);
  }
  return Status::OK();
}

Status DB::Impl::Delete(const WriteOptions& options, std::string key) {
  std::lock_guard lock(mu_);
  metrics_.deletes.fetch_add(1, std::memory_order_relaxed);
  const std::uint64_t seq = next_seq_++;
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendDelete(seq, key, sync_now);
  if (!s.ok()) return s;
  s = mem_->Delete(seq, key);
  if (!s.ok()) return s;
  if (sync_now) metrics_.wal_syncs.fetch_add(1, std::memory_order_relaxed);
  if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    auto imm_mem = std::move(mem_);
    mem_ = std::make_unique<MemTable>();
    std::filesystem::path rotated;
    s = RotateWalLocked(seq, rotated);
    if (!s.ok()) return s;
    EnqueueImmutable(std::move(imm_mem), seq, rotated);
  }
  return Status::OK();
}

Status DB::Impl::Write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::OK();
  std::lock_guard lock(mu_);
  metrics_.batches.fetch_add(1, std::memory_order_relaxed);
  std::vector<std::uint64_t> seqs;
  seqs.reserve(batch.ops().size());

  for (std::size_t i = 0; i < batch.ops().size(); ++i) {
    seqs.push_back(next_seq_++);
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

  if (mem_->ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    auto imm_mem = std::move(mem_);
    mem_ = std::make_unique<MemTable>();
    std::filesystem::path rotated;
    Status s = RotateWalLocked(seqs.back(), rotated);
    if (!s.ok()) return s;
    EnqueueImmutable(std::move(imm_mem), seqs.back(), rotated);
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

Status DB::Impl::Scan(const ReadOptions& /*options*/, std::string_view from, std::size_t limit,
                      std::vector<std::pair<std::string, std::string>>& out) {
  if (limit == 0) return Status::OK();
  std::map<std::string, MemEntry> merged;

  auto mem_entries = mem_->Snapshot();
  for (auto& e : mem_entries) {
    merged[e.key] = std::move(e);
  }

  std::shared_lock lock(sstable_mu_);
  for (const auto& level : levels_) {
    for (const auto& tref : level) {
      std::vector<MemEntry> entries;
      Status s = tref.table->LoadAll(entries);
      if (!s.ok()) return s;
      for (auto& e : entries) {
        auto it = merged.find(e.key);
        if (it == merged.end() || it->second.seq < e.seq) {
          merged[e.key] = std::move(e);
        }
      }
    }
  }

  auto it = merged.lower_bound(std::string(from));
  std::size_t added = 0;
  for (; it != merged.end() && added < limit; ++it) {
    if (!it->second.deleted) {
      out.emplace_back(it->first, it->second.value);
      ++added;
    }
  }

  return Status::OK();
}

Status DB::Impl::Flush() {
  std::lock_guard lock(mu_);
  Status s = FlushLocked();
  if (!s.ok()) return s;
  return MaybeCompactLocked();
}

Status DB::Impl::FlushLocked() {
  if (mem_->Empty() && immutables_.empty()) return Status::OK();
  if (!mem_->Empty()) {
    auto imm_mem = std::move(mem_);
    mem_ = std::make_unique<MemTable>();
    std::filesystem::path rotated;
    std::uint64_t max_seq = next_seq_ ? next_seq_ - 1 : 0;
    Status s = RotateWalLocked(max_seq, rotated);
    if (!s.ok()) return s;
    EnqueueImmutable(std::move(imm_mem), max_seq, rotated);
  }
  std::unique_lock lk(flush_mu_);
  flush_cv_.wait(lk, [this] { return immutables_.empty(); });
  return Status::OK();
}

Status DB::Impl::WriteManifest() {
  std::vector<std::tuple<std::size_t, std::string, std::string, std::string, std::uint64_t, std::uint64_t>> entries;
  for (std::size_t lvl = 0; lvl < levels_.size(); ++lvl) {
    for (const auto& t : levels_[lvl]) {
      auto rel = t.table->path().lexically_relative(data_dir_).string();
      if (rel.empty() || rel[0] == '.') rel = t.table->path().filename().string();
      entries.emplace_back(lvl, rel, t.min_key, t.max_key, t.size, t.table->max_sequence());
    }
  }

  const auto manifest_tmp = data_dir_ / "MANIFEST.tmp";
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
    Status s = SSTable::Open(path, block_cache_, table);
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

Status DB::Impl::MaybeCompactLocked() {
  while (true) {
    bool need_l0 = false;
    bool need_other = false;
    std::size_t target_level = 0;
    {
      std::shared_lock lock(sstable_mu_);
      if (!levels_.empty() && levels_[0].size() > options_.level0_file_limit) {
        need_l0 = true;
      } else {
        for (std::size_t lvl = 1; lvl < levels_.size(); ++lvl) {
          if (LevelBytes(lvl) > LevelMaxBytes(lvl)) {
            need_other = true;
            target_level = lvl;
            break;
          }
        }
      }
    }

    if (need_l0) {
      auto s = CompactLevel(0);
      if (!s.ok()) return s;
      continue;
    }
    if (need_other) {
      auto s = CompactLevel(target_level);
      if (!s.ok()) return s;
      continue;
    }
    break;
  }
  return Status::OK();
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
    std::lock_guard lk(flush_mu_);
    immutables_.push_back(ImmutableMem{std::move(mem), std::move(wal_path), max_seq});
  }
  flush_cv_.notify_one();
}

void DB::Impl::MaybeScheduleFlush() {
  // No-op: flush thread waits on condition variable; notify is done in EnqueueImmutable.
}

Status DB::Impl::FlushImmutable(ImmutableMem&& imm) {
  auto start = std::chrono::steady_clock::now();
  auto entries = imm.mem->Snapshot();
  if (entries.empty()) {
    std::error_code ec;
    std::filesystem::remove(imm.wal_path, ec);
    return Status::OK();
  }

  std::uint64_t max_seq = 0;
  for (const auto& e : entries) max_seq = std::max(max_seq, e.seq);
  const auto filename = "sst-l0-" + std::to_string(max_seq) + ".sst";
  const auto path = sst_dir_ / filename;

  Status s =
      SSTable::Write(path, entries, options_.sstable_block_size_bytes, options_.bloom_bits_per_key);
  if (!s.ok()) return s;

  std::shared_ptr<SSTable> table;
  s = SSTable::Open(path, block_cache_, table);
  if (!s.ok()) return s;

  {
    std::unique_lock lock_tables(sstable_mu_);
    if (levels_.empty()) levels_.resize(1);
    levels_[0].insert(levels_[0].begin(),
                      TableRef{table, table->min_key(), table->max_key(), table->file_size()});
    std::sort(levels_[0].begin(), levels_[0].end(),
              [](const TableRef& a, const TableRef& b) { return a.table->max_sequence() > b.table->max_sequence(); });
  }
  Status ms = WriteManifest();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
                     .count();
  metrics_.flushes.fetch_add(1, std::memory_order_relaxed);
  metrics_.flush_ms.fetch_add(static_cast<std::uint64_t>(elapsed), std::memory_order_relaxed);
  metrics_.flush_bytes.fetch_add(table->file_size(), std::memory_order_relaxed);

  std::error_code ec;
  std::filesystem::remove(imm.wal_path, ec);

  // Try to compact while holding mu_ to reuse existing logic; best-effort.
  std::unique_lock lk(mu_, std::try_to_lock);
  if (lk.owns_lock()) {
    MaybeCompactLocked();
  }
  return ms;
}

void DB::Impl::FlushThreadLoop() {
  while (true) {
    ImmutableMem imm;
    {
      std::unique_lock lk(flush_mu_);
      flush_cv_.wait(lk, [this] { return stop_flush_ || !immutables_.empty(); });
      if (stop_flush_ && immutables_.empty()) break;
      imm = std::move(immutables_.front());
      immutables_.erase(immutables_.begin());
    }
    Status s = FlushImmutable(std::move(imm));
    if (!s.ok()) {
      // Best-effort log.
      std::cerr << "FlushImmutable failed: " << s.ToString() << "\n";
    }
    {
      std::lock_guard lk(flush_mu_);
      if (immutables_.empty()) {
        flush_cv_.notify_all();
      }
    }
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
    inputs = levels_[level];
    if (inputs.empty()) return Status::OK();
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
  }

  std::vector<MemEntry> merged_entries;
  merged_entries.reserve(inputs.size() * 16);

  auto load_tables = [&](const std::vector<TableRef>& tables) -> Status {
    for (const auto& t : tables) {
      Status s = t.table->LoadAll(merged_entries);
      if (!s.ok()) return s;
    }
    return Status::OK();
  };

  Status s = load_tables(inputs);
  if (!s.ok()) return s;
  s = load_tables(next_inputs);
  if (!s.ok()) return s;

  if (merged_entries.empty()) return Status::OK();

  std::sort(merged_entries.begin(), merged_entries.end(),
            [](const MemEntry& a, const MemEntry& b) {
              if (a.key == b.key) return a.seq > b.seq;
              return a.key < b.key;
            });
  std::vector<MemEntry> compacted;
  compacted.reserve(merged_entries.size());
  std::string current_key;
  for (const auto& e : merged_entries) {
    if (compacted.empty() || e.key != current_key) {
      compacted.push_back(e);
      current_key = e.key;
    }
  }

  auto write_outputs = [&](const std::vector<MemEntry>& entries_out, std::size_t target_level,
                           std::vector<TableRef>& out_tables) -> Status {
    if (entries_out.empty()) return Status::OK();
    std::vector<MemEntry> chunk;
    std::uint64_t approx_size = 0;
    auto flush_chunk = [&](std::vector<MemEntry>& chunk_data) -> Status {
      if (chunk_data.empty()) return Status::OK();
      std::uint64_t max_seq = 0;
      for (const auto& e : chunk_data) max_seq = std::max(max_seq, e.seq);
      const auto filename =
          "sst-l" + std::to_string(target_level) + "-" + std::to_string(max_seq) + "-" +
          std::to_string(out_tables.size()) + ".sst";
      const auto path = sst_dir_ / filename;
      Status ws = SSTable::Write(path, chunk_data, options_.sstable_block_size_bytes,
                                 options_.bloom_bits_per_key);
      if (!ws.ok()) return ws;
      std::shared_ptr<SSTable> t;
      ws = SSTable::Open(path, block_cache_, t);
      if (!ws.ok()) return ws;
      out_tables.push_back(TableRef{t, t->min_key(), t->max_key(), t->file_size()});
      chunk_data.clear();
      approx_size = 0;
      return Status::OK();
    };

    for (const auto& e : entries_out) {
      std::uint64_t entry_sz = 1 + 8 + 4 + 4 + e.key.size() + e.value.size();
      chunk.push_back(e);
      approx_size += entry_sz;
      if (approx_size >= options_.sstable_target_size_bytes) {
        Status fs = flush_chunk(chunk);
        if (!fs.ok()) return fs;
      }
    }
    return flush_chunk(chunk);
  };

  std::vector<TableRef> outputs;
  s = write_outputs(compacted, level + 1, outputs);
  if (!s.ok()) return s;

  std::uint64_t input_bytes = 0;
  for (const auto& t : inputs) input_bytes += t.size;
  for (const auto& t : next_inputs) input_bytes += t.size;
  std::uint64_t output_bytes = 0;

  std::unique_lock lock(sstable_mu_);
  if (levels_.size() <= level + 1) levels_.resize(level + 2);
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
  std::lock_guard lock(mu_);
  // Flush memtable first to simplify compaction.
  Status s = FlushLocked();
  if (!s.ok()) return s;
  return MaybeCompactLocked();
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
  if (flush_thread_.joinable()) flush_thread_.join();
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

Status DB::Scan(const ReadOptions& options, std::string_view from, std::size_t limit,
                std::vector<std::pair<std::string, std::string>>& out) {
  return impl_->Scan(options, from, limit, out);
}

Metrics DB::GetMetrics() const { return impl_->GetMetrics(); }

}  // namespace dkv
