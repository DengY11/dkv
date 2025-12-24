#include "dkv/db.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

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
        sst_dir_(data_dir_ / "sst") {}

  Status Init();

  Status Put(const WriteOptions& options, std::string key, std::string value);
  Status Delete(const WriteOptions& options, std::string key);
  Status Write(const WriteOptions& options, const WriteBatch& batch);
  Status Get(const ReadOptions& options, std::string_view key, std::string& value);
  Status Flush();
  Status Compact();
  Status Scan(const ReadOptions& options, std::string_view from, std::size_t limit,
              std::vector<std::pair<std::string, std::string>>& out);

 private:
  Status FlushLocked();
  Status LoadSSTables();
  Status MaybeCompactLocked();
  Status CompactLevel(std::size_t level);
  std::uint64_t LevelMaxBytes(std::size_t level) const;
  std::uint64_t LevelBytes(std::size_t level) const;

  Options options_;
  std::filesystem::path data_dir_;
  std::filesystem::path wal_path_;
  std::filesystem::path sst_dir_;

  std::unique_ptr<WAL> wal_;
  MemTable mem_;
  std::shared_ptr<BlockCache> block_cache_;

  std::mutex mu_;  // guards mem_, wal_ operations, seq_ and flush/compaction exclusivity.
  std::uint64_t next_seq_{1};

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

  wal_ = std::make_unique<WAL>(wal_path_, options_.sync_wal);
  Status s = wal_->Open();
  if (!s.ok()) return s;

  s = LoadSSTables();
  if (!s.ok()) return s;

  std::uint64_t max_seq_seen = 0;
  s = wal_->Replay([this, &max_seq_seen](std::uint64_t seq, bool deleted, std::string&& key,
                                         std::string&& value) {
    max_seq_seen = std::max(max_seq_seen, seq);
    if (deleted) {
      mem_.Delete(seq, key);
    } else {
      mem_.Put(seq, key, value);
    }
  });
  if (!s.ok()) return s;

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
  return Status::OK();
}

Status DB::Impl::LoadSSTables() {
  std::vector<std::vector<TableRef>> loaded;
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
  return Status::OK();
}

Status DB::Impl::Put(const WriteOptions& options, std::string key, std::string value) {
  std::lock_guard lock(mu_);
  const std::uint64_t seq = next_seq_++;
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendPut(seq, key, value, sync_now);
  if (!s.ok()) return s;
  s = mem_.Put(seq, key, value);
  if (!s.ok()) return s;
  if (mem_.ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    s = FlushLocked();
    if (!s.ok()) return s;
    return MaybeCompactLocked();
  }
  return Status::OK();
}

Status DB::Impl::Delete(const WriteOptions& options, std::string key) {
  std::lock_guard lock(mu_);
  const std::uint64_t seq = next_seq_++;
  const bool sync_now = options.sync || options_.sync_wal;
  Status s = wal_->AppendDelete(seq, key, sync_now);
  if (!s.ok()) return s;
  s = mem_.Delete(seq, key);
  if (!s.ok()) return s;
  if (mem_.ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    s = FlushLocked();
    if (!s.ok()) return s;
    return MaybeCompactLocked();
  }
  return Status::OK();
}

Status DB::Impl::Write(const WriteOptions& options, const WriteBatch& batch) {
  if (batch.empty()) return Status::OK();
  std::lock_guard lock(mu_);
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
      s = mem_.Put(seq, op.key, op.value);
    } else {
      s = wal_->AppendDelete(seq, op.key, /*sync=*/false);
      if (!s.ok()) return s;
      s = mem_.Delete(seq, op.key);
    }
    if (!s.ok()) return s;
  }

  const bool need_sync = options.sync || options_.sync_wal;
  if (need_sync) {
    Status s = wal_->Sync(/*force_sync=*/true);
    if (!s.ok()) return s;
  }

  if (mem_.ApproximateMemoryUsage() >= options_.memtable_soft_limit_bytes) {
    Status s = FlushLocked();
    if (!s.ok()) return s;
    return MaybeCompactLocked();
  }
  return Status::OK();
}

Status DB::Impl::Get(const ReadOptions& /*options*/, std::string_view key, std::string& value) {
  // Check memtable first (newest).
  MemEntry entry;
  if (mem_.Get(key, entry)) {
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

  auto mem_entries = mem_.Snapshot();
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
  if (mem_.Empty()) return Status::OK();

  auto entries = mem_.Snapshot();  // Already sorted by key.
  if (entries.empty()) return Status::OK();

  std::uint64_t max_seq = 0;
  for (const auto& e : entries) {
    max_seq = std::max(max_seq, e.seq);
  }

  const auto filename = "sst-l0-" + std::to_string(max_seq) + ".sst";
  const auto path = sst_dir_ / filename;

  Status s =
      SSTable::Write(path, entries, options_.sstable_block_size_bytes, options_.bloom_bits_per_key);
  if (!s.ok()) return s;

  std::shared_ptr<SSTable> table;
  s = SSTable::Open(path, block_cache_, table);
  if (!s.ok()) return s;

  mem_.Clear();
  s = wal_->Reset();
  if (!s.ok()) return s;

  std::unique_lock lock_tables(sstable_mu_);
  if (levels_.empty()) levels_.resize(1);
  levels_[0].insert(levels_[0].begin(),
                    TableRef{table, table->min_key(), table->max_key(), table->file_size()});
  std::sort(levels_[0].begin(), levels_[0].end(),
            [](const TableRef& a, const TableRef& b) { return a.table->max_sequence() > b.table->max_sequence(); });
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

static bool Overlaps(const std::string& a_min, const std::string& a_max, const std::string& b_min,
                     const std::string& b_max) {
  return !(a_max < b_min || b_max < a_min);
}

Status DB::Impl::CompactLevel(std::size_t level) {
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
  return Status::OK();
}

Status DB::Impl::Compact() {
  std::lock_guard lock(mu_);
  // Flush memtable first to simplify compaction.
  Status s = FlushLocked();
  if (!s.ok()) return s;
  return MaybeCompactLocked();
}

DB::DB(Options options) : impl_(new Impl(std::move(options))) {}
DB::~DB() = default;

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

}  // namespace dkv
