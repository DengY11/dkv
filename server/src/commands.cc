#include "commands.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <dkv/filename.h>

#include "resp.h"
#include "server.h"

namespace dkv_server {
namespace {

std::string UpperAscii(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) out.push_back(static_cast<char>(std::toupper(c)));
  return out;
}

std::string LowerAscii(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) out.push_back(static_cast<char>(std::tolower(c)));
  return out;
}

bool GlobMatch(std::string_view pattern, std::string_view text) {
  std::size_t p = 0;
  std::size_t t = 0;
  std::size_t star = std::string_view::npos;
  std::size_t match = 0;
  while (t < text.size()) {
    if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == text[t])) {
      ++p;
      ++t;
      continue;
    }
    if (p < pattern.size() && pattern[p] == '*') {
      star = p++;
      match = t;
      continue;
    }
    if (star != std::string_view::npos) {
      p = star + 1;
      t = ++match;
      continue;
    }
    return false;
  }
  while (p < pattern.size() && pattern[p] == '*') ++p;
  return p == pattern.size();
}

bool ParseInt(std::string_view s, std::int64_t* out) {
  if (s.empty()) return false;
  std::int64_t v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size()) return false;
  *out = v;
  return true;
}

bool ParseUint64(std::string_view s, std::uint64_t* out) {
  if (s.empty()) return false;
  std::uint64_t v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size()) return false;
  *out = v;
  return true;
}

CommandResult CmdPing(const std::vector<std::string>& args, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  if (args.size() == 1) {
    resp::AppendSimpleString(r.payload, "PONG");
  } else {
    resp::AppendBulkString(r.payload, args[1]);
  }
  return r;
}

CommandResult CmdEcho(const std::vector<std::string>& args, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'echo' command");
    return r;
  }
  resp::AppendBulkString(r.payload, args[1]);
  return r;
}

CommandResult CmdQuit(const std::vector<std::string>&, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  resp::AppendSimpleString(r.payload, "OK");
  r.close_after = true;
  return r;
}

CommandResult CmdCommand(const std::vector<std::string>&, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  resp::AppendArrayHeader(r.payload, 0);
  return r;
}

CommandResult CmdClient(const std::vector<std::string>&, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  resp::AppendSimpleString(r.payload, "OK");
  return r;
}



CommandResult CmdHello(const std::vector<std::string>&, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  resp::AppendArrayHeader(r.payload, 14);
  resp::AppendBulkString(r.payload, "server");
  resp::AppendBulkString(r.payload, "dkv");
  resp::AppendBulkString(r.payload, "version");
  resp::AppendBulkString(r.payload, "0.1");
  resp::AppendBulkString(r.payload, "proto");
  resp::AppendBulkString(r.payload, "2");
  resp::AppendBulkString(r.payload, "id");
  resp::AppendBulkString(r.payload, "0");
  resp::AppendBulkString(r.payload, "mode");
  resp::AppendBulkString(r.payload, "standalone");
  resp::AppendBulkString(r.payload, "role");
  resp::AppendBulkString(r.payload, "master");
  resp::AppendBulkString(r.payload, "modules");
  resp::AppendArrayHeader(r.payload, 0);
  return r;
}

CommandResult CmdInfo(const std::vector<std::string>&, dkv::DB*, const ServerConfig*) {
  CommandResult r;
  std::string info;
  info += "# Server\n";
  info += "redis_version:0.1\n";
  info += "dkv_server:dkv-server\n";
  resp::AppendBulkString(r.payload, info);
  return r;
}

CommandResult CmdConfig(const std::vector<std::string>& args, dkv::DB*, const ServerConfig* cfg) {
  CommandResult r;
  if (args.size() < 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'config' command");
    return r;
  }
  const std::string sub = UpperAscii(args[1]);
  if (sub == "GET") {
    if (args.size() != 3) {
      resp::AppendError(r.payload, "ERR wrong number of arguments for 'config get' command");
      return r;
    }
    const std::string pat = LowerAscii(args[2]);
    const auto& o = cfg->dkv_options;
    auto yesno = [](bool b) { return b ? "yes" : "no"; };

    std::vector<std::pair<std::string, std::string>> entries;
    entries.emplace_back("bind", cfg->bind);
    entries.emplace_back("port", std::to_string(cfg->port));
    entries.emplace_back("subreactors", std::to_string(cfg->subreactors));
    entries.emplace_back("workers", std::to_string(cfg->workers));

    entries.emplace_back("data-dir", o.data_dir.string());
    entries.emplace_back("wal-path", (o.data_dir / std::string(dkv::kWalActiveName)).string());
    entries.emplace_back("sst-dir", (o.data_dir / "sst").string());
    entries.emplace_back("manifest-path", (o.data_dir / std::string(dkv::kManifestName)).string());
    // Minimal Redis-compatible config keys (best-effort, for tools like redis-benchmark).
    entries.emplace_back("dir", o.data_dir.string());
    entries.emplace_back("dbfilename", "");
    entries.emplace_back("appendonly", "no");
    entries.emplace_back("save", "");

    entries.emplace_back("memtable-soft-limit-bytes", std::to_string(o.memtable_soft_limit_bytes));
    entries.emplace_back("sync-wal", yesno(o.sync_wal));
    entries.emplace_back("sstable-target-size-bytes", std::to_string(o.sstable_target_size_bytes));
    entries.emplace_back("sstable-block-size-bytes", std::to_string(o.sstable_block_size_bytes));
    entries.emplace_back("bloom-bits-per-key", std::to_string(o.bloom_bits_per_key));
    entries.emplace_back("bloom-cache-capacity-bytes", std::to_string(o.bloom_cache_capacity_bytes));
    entries.emplace_back("raw-block-cache-capacity-bytes", std::to_string(o.raw_block_cache_capacity_bytes));
    entries.emplace_back("block-cache-capacity-bytes", std::to_string(o.block_cache_capacity_bytes));
    entries.emplace_back("level0-file-limit", std::to_string(o.level0_file_limit));
    entries.emplace_back("level-base-bytes", std::to_string(o.level_base_bytes));
    entries.emplace_back("level-size-multiplier", std::to_string(o.level_size_multiplier));
    entries.emplace_back("max-levels", std::to_string(o.max_levels));
    entries.emplace_back("flush-thread-count", std::to_string(o.flush_thread_count));
    entries.emplace_back("max-immutable-memtables", std::to_string(o.max_immutable_memtables));
    entries.emplace_back("compaction-thread-count", std::to_string(o.compaction_thread_count));
    entries.emplace_back("wal-sync-interval-ms", std::to_string(o.wal_sync_interval_ms));
    entries.emplace_back("enable-crc", yesno(o.enable_crc));
    entries.emplace_back("verify-sstable-crc", yesno(o.verify_sstable_crc));
    entries.emplace_back("enable-compress", yesno(o.enable_compress));

    std::vector<std::pair<std::string, std::string>> matched;
    matched.reserve(entries.size());
    for (auto& kv : entries) {
      if (GlobMatch(pat, LowerAscii(kv.first))) matched.push_back(kv);
    }

    resp::AppendArrayHeader(r.payload, matched.size() * 2);
    for (auto& kv : matched) {
      resp::AppendBulkString(r.payload, kv.first);
      resp::AppendBulkString(r.payload, kv.second);
    }
    return r;
  }

  if (sub == "RESETSTAT" || sub == "REWRITE") {
    resp::AppendSimpleString(r.payload, "OK");
    return r;
  }

  resp::AppendError(r.payload, "ERR unsupported CONFIG subcommand");
  return r;
}

CommandResult CmdGet(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'get' command");
    return r;
  }
  std::string value;
  dkv::Status s = db->Get(dkv::ReadOptions{}, args[1], value);
  if (s.ok()) {
    resp::AppendBulkString(r.payload, value);
  } else if (s.code() == dkv::Status::Code::kNotFound) {
    resp::AppendNullBulkString(r.payload);
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
  }
  return r;
}

CommandResult CmdSet(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 3) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'set' command");
    return r;
  }
  dkv::Status s = db->Put(dkv::WriteOptions{}, args[1], args[2]);
  if (s.ok()) {
    resp::AppendSimpleString(r.payload, "OK");
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
  }
  return r;
}

CommandResult CmdDel(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() < 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'del' command");
    return r;
  }
  std::int64_t removed = 0;
  for (std::size_t i = 1; i < args.size(); ++i) {
    std::string tmp;
    dkv::Status gs = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], tmp);
    if (gs.ok()) {
      dkv::Status ds = db->Delete(dkv::WriteOptions{}, args[i]);
      if (!ds.ok()) {
        resp::AppendError(r.payload, std::string("ERR ") + ds.ToString());
        return r;
      }
      ++removed;
    } else if (gs.code() == dkv::Status::Code::kNotFound) {
      // noop
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + gs.ToString());
      return r;
    }
  }
  resp::AppendInteger(r.payload, removed);
  return r;
}

CommandResult CmdExists(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() < 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'exists' command");
    return r;
  }
  std::int64_t cnt = 0;
  for (std::size_t i = 1; i < args.size(); ++i) {
    std::string tmp;
    dkv::Status s = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], tmp);
    if (s.ok()) {
      ++cnt;
    } else if (s.code() == dkv::Status::Code::kNotFound) {
      // noop
    } else {
      resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
      return r;
    }
  }
  resp::AppendInteger(r.payload, cnt);
  return r;
}

CommandResult CmdMget(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() < 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'mget' command");
    return r;
  }
  resp::AppendArrayHeader(r.payload, args.size() - 1);
  for (std::size_t i = 1; i < args.size(); ++i) {
    std::string value;
    dkv::Status s = db->Get(dkv::ReadOptions{.fill_cache = false}, args[i], value);
    if (s.ok()) {
      resp::AppendBulkString(r.payload, value);
    } else if (s.code() == dkv::Status::Code::kNotFound) {
      resp::AppendNullBulkString(r.payload);
    } else {
      r.payload.clear();
      resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
      return r;
    }
  }
  return r;
}

CommandResult CmdMset(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() < 3 || ((args.size() - 1) % 2) != 0) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'mset' command");
    return r;
  }
  dkv::WriteBatch batch;
  for (std::size_t i = 1; i < args.size(); i += 2) {
    batch.Put(args[i], args[i + 1]);
  }
  dkv::Status s = db->Write(dkv::WriteOptions{}, batch);
  if (s.ok()) {
    resp::AppendSimpleString(r.payload, "OK");
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
  }
  return r;
}

CommandResult DoIncr(const std::string& key, std::int64_t by, dkv::DB* db) {
  CommandResult r;
  std::string cur;
  dkv::Status gs = db->Get(dkv::ReadOptions{.fill_cache = false}, key, cur);
  std::int64_t val = 0;
  if (gs.ok()) {
    if (!ParseInt(cur, &val)) {
      resp::AppendError(r.payload, "ERR value is not an integer or out of range");
      return r;
    }
  } else if (gs.code() == dkv::Status::Code::kNotFound) {
    val = 0;
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + gs.ToString());
    return r;
  }
  if ((by > 0 && val > std::numeric_limits<std::int64_t>::max() - by) ||
      (by < 0 && val < std::numeric_limits<std::int64_t>::min() - by)) {
    resp::AppendError(r.payload, "ERR increment or decrement would overflow");
    return r;
  }
  val += by;
  dkv::Status ps = db->Put(dkv::WriteOptions{}, key, std::to_string(val));
  if (!ps.ok()) {
    resp::AppendError(r.payload, std::string("ERR ") + ps.ToString());
    return r;
  }
  resp::AppendInteger(r.payload, val);
  return r;
}

CommandResult CmdIncr(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'incr' command");
    return r;
  }
  return DoIncr(args[1], 1, db);
}

CommandResult CmdDecr(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'decr' command");
    return r;
  }
  return DoIncr(args[1], -1, db);
}

CommandResult CmdIncrBy(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 3) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'incrby' command");
    return r;
  }
  std::int64_t by = 0;
  if (!ParseInt(args[2], &by)) {
    resp::AppendError(r.payload, "ERR value is not an integer or out of range");
    return r;
  }
  return DoIncr(args[1], by, db);
}

CommandResult CmdDecrBy(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig*) {
  CommandResult r;
  if (args.size() != 3) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'decrby' command");
    return r;
  }
  std::int64_t by = 0;
  if (!ParseInt(args[2], &by)) {
    resp::AppendError(r.payload, "ERR value is not an integer or out of range");
    return r;
  }
  return DoIncr(args[1], -by, db);
}

struct IteratorEntry {
  std::mutex mu;
  std::unique_ptr<dkv::DB::Iterator> it;
};

class IteratorRegistry {
 public:
  std::uint64_t Create(std::unique_ptr<dkv::DB::Iterator> it) {
    auto entry = std::make_shared<IteratorEntry>();
    entry->it = std::move(it);
    const std::uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
    {
      std::lock_guard lk(mu_);
      iters_.emplace(id, std::move(entry));
    }
    return id;
  }

  std::shared_ptr<IteratorEntry> Get(std::uint64_t id) {
    std::lock_guard lk(mu_);
    auto it = iters_.find(id);
    if (it == iters_.end()) return {};
    return it->second;
  }

  bool Erase(std::uint64_t id) {
    std::lock_guard lk(mu_);
    return iters_.erase(id) != 0;
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::uint64_t, std::shared_ptr<IteratorEntry>> iters_;
  std::atomic<std::uint64_t> next_id_{1};
};

IteratorRegistry& GlobalIterators() {
  static IteratorRegistry reg;
  return reg;
}

void AppendIterState(std::string& out, const dkv::DB::Iterator& it) {
  const bool valid = it.Valid();
  resp::AppendArrayHeader(out, 3);
  resp::AppendInteger(out, valid ? 1 : 0);
  if (valid) {
    resp::AppendBulkString(out, it.key());
    resp::AppendBulkString(out, it.value());
  } else {
    resp::AppendNullBulkString(out);
    resp::AppendNullBulkString(out);
  }
}

CommandResult CmdHiDylan(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)args;
  (void)db;
  (void)cfg;
  CommandResult r;
  resp::AppendSimpleString(r.payload, "Hi, i am dkv author, nice to see you use dkv!!");
  return r;
}

CommandResult CmdMetric(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)cfg;
  CommandResult r;
  if (args.size() != 1) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'metrics' command");
    return r;
  }
  auto m = db->GetMetrics();

  std::vector<std::pair<std::string, std::string>> entries;
  entries.emplace_back("puts", std::to_string(m.puts));
  entries.emplace_back("deletes", std::to_string(m.deletes));
  entries.emplace_back("gets", std::to_string(m.gets));
  entries.emplace_back("batches", std::to_string(m.batches));
  entries.emplace_back("flushes", std::to_string(m.flushes));
  entries.emplace_back("flush_ms", std::to_string(m.flush_ms));
  entries.emplace_back("flush_bytes", std::to_string(m.flush_bytes));
  entries.emplace_back("compactions", std::to_string(m.compactions));
  entries.emplace_back("compaction_ms", std::to_string(m.compaction_ms));
  entries.emplace_back("compaction_input_bytes", std::to_string(m.compaction_input_bytes));
  entries.emplace_back("compaction_output_bytes", std::to_string(m.compaction_output_bytes));
  entries.emplace_back("wal_syncs", std::to_string(m.wal_syncs));
  entries.emplace_back("sstable_crc_errors", std::to_string(m.sstable_crc_errors));
  entries.emplace_back("sstable_read_errors", std::to_string(m.sstable_read_errors));
  entries.emplace_back("block_cache_hits", std::to_string(m.block_cache_hits));
  entries.emplace_back("block_cache_misses", std::to_string(m.block_cache_misses));
  entries.emplace_back("block_cache_puts", std::to_string(m.block_cache_puts));
  entries.emplace_back("block_cache_evictions", std::to_string(m.block_cache_evictions));
  entries.emplace_back("block_cache_used_bytes", std::to_string(m.block_cache_used_bytes));
  entries.emplace_back("block_cache_capacity_bytes", std::to_string(m.block_cache_capacity_bytes));

  resp::AppendArrayHeader(r.payload, entries.size() * 2);
  for (const auto& [k, v] : entries) {
    resp::AppendBulkString(r.payload, k);
    resp::AppendBulkString(r.payload, v);
  }
  return r;
}

CommandResult CmdCompact(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)cfg;
  CommandResult r;
  if (args.size() != 1) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'compact' command");
    return r;
  }
  dkv::Status s = db->Compact();
  if (s.ok()) {
    resp::AppendSimpleString(r.payload, "OK");
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
  }
  return r;
}

CommandResult CmdFlush(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)cfg;
  CommandResult r;
  if (args.size() != 1) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'flush' command");
    return r;
  }
  dkv::Status s = db->Flush();
  if (s.ok()) {
    resp::AppendSimpleString(r.payload, "OK");
  } else {
    resp::AppendError(r.payload, std::string("ERR ") + s.ToString());
  }
  return r;
}

CommandResult CmdScan(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)cfg;
  CommandResult r;
  if (args.size() > 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'scan' command");
    return r;
  }
  std::string_view prefix;
  if (args.size() == 2) prefix = args[1];

  auto it = db->Scan(dkv::ReadOptions{}, prefix);
  if (!it) {
    resp::AppendError(r.payload, "ERR scan failed");
    return r;
  }
  dkv::Status st = it->status();
  if (!st.ok()) {
    resp::AppendError(r.payload, std::string("ERR ") + st.ToString());
    return r;
  }

  const std::uint64_t id = GlobalIterators().Create(std::move(it));
  auto entry = GlobalIterators().Get(id);
  if (!entry) {
    resp::AppendError(r.payload, "ERR failed to register iterator");
    return r;
  }

  std::lock_guard lk(entry->mu);
  resp::AppendArrayHeader(r.payload, 4);
  resp::AppendInteger(r.payload, static_cast<std::int64_t>(id));
  resp::AppendInteger(r.payload, entry->it && entry->it->Valid() ? 1 : 0);
  if (entry->it && entry->it->Valid()) {
    resp::AppendBulkString(r.payload, entry->it->key());
    resp::AppendBulkString(r.payload, entry->it->value());
  } else {
    resp::AppendNullBulkString(r.payload);
    resp::AppendNullBulkString(r.payload);
  }
  return r;
}

CommandResult CmdSeekToFirst(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)db;
  (void)cfg;
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'seektofirst' command");
    return r;
  }
  std::uint64_t id = 0;
  if (!ParseUint64(args[1], &id)) {
    resp::AppendError(r.payload, "ERR invalid iterator id");
    return r;
  }
  auto entry = GlobalIterators().Get(id);
  if (!entry) {
    resp::AppendError(r.payload, "ERR iterator not found");
    return r;
  }
  std::lock_guard lk(entry->mu);
  if (!entry->it) {
    resp::AppendError(r.payload, "ERR iterator not initialized");
    return r;
  }
  entry->it->SeekToFirst();
  AppendIterState(r.payload, *entry->it);
  return r;
}

CommandResult CmdSeek(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)db;
  (void)cfg;
  CommandResult r;
  if (args.size() != 3) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'seek' command");
    return r;
  }
  std::uint64_t id = 0;
  if (!ParseUint64(args[1], &id)) {
    resp::AppendError(r.payload, "ERR invalid iterator id");
    return r;
  }
  auto entry = GlobalIterators().Get(id);
  if (!entry) {
    resp::AppendError(r.payload, "ERR iterator not found");
    return r;
  }
  std::lock_guard lk(entry->mu);
  if (!entry->it) {
    resp::AppendError(r.payload, "ERR iterator not initialized");
    return r;
  }
  entry->it->Seek(args[2]);
  AppendIterState(r.payload, *entry->it);
  return r;
}

CommandResult CmdValid(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)db;
  (void)cfg;
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'valid' command");
    return r;
  }
  std::uint64_t id = 0;
  if (!ParseUint64(args[1], &id)) {
    resp::AppendError(r.payload, "ERR invalid iterator id");
    return r;
  }
  auto entry = GlobalIterators().Get(id);
  if (!entry) {
    resp::AppendError(r.payload, "ERR iterator not found");
    return r;
  }
  std::lock_guard lk(entry->mu);
  if (!entry->it) {
    resp::AppendError(r.payload, "ERR iterator not initialized");
    return r;
  }
  resp::AppendInteger(r.payload, entry->it->Valid() ? 1 : 0);
  return r;
}

CommandResult CmdNext(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)db;
  (void)cfg;
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'next' command");
    return r;
  }
  std::uint64_t id = 0;
  if (!ParseUint64(args[1], &id)) {
    resp::AppendError(r.payload, "ERR invalid iterator id");
    return r;
  }
  auto entry = GlobalIterators().Get(id);
  if (!entry) {
    resp::AppendError(r.payload, "ERR iterator not found");
    return r;
  }
  std::lock_guard lk(entry->mu);
  if (!entry->it) {
    resp::AppendError(r.payload, "ERR iterator not initialized");
    return r;
  }
  entry->it->Next();
  AppendIterState(r.payload, *entry->it);
  return r;
}

CommandResult CmdIterDel(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  (void)db;
  (void)cfg;
  CommandResult r;
  if (args.size() != 2) {
    resp::AppendError(r.payload, "ERR wrong number of arguments for 'iterdel' command");
    return r;
  }
  std::uint64_t id = 0;
  if (!ParseUint64(args[1], &id)) {
    resp::AppendError(r.payload, "ERR invalid iterator id");
    return r;
  }
  if (!GlobalIterators().Erase(id)) {
    resp::AppendError(r.payload, "ERR iterator not found");
    return r;
  }
  resp::AppendSimpleString(r.payload, "OK");
  return r;
}

using Handler = CommandResult (*)(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg);

struct CommandSpec {
  Handler handler{nullptr};
  bool requires_db{false};
  bool requires_cfg{false};
};

class CommandRegistry {
 public:
  void Register(std::string name_upper, CommandSpec spec) { commands_.emplace(std::move(name_upper), spec); }

  [[nodiscard]] const CommandSpec* Lookup(const std::string& name_upper) const {
    auto it = commands_.find(name_upper);
    if (it == commands_.end()) return nullptr;
    return &it->second;
  }

 private:
  std::unordered_map<std::string, CommandSpec> commands_;
};

const CommandRegistry& DefaultRegistry() {
  static const CommandRegistry reg = [] {
    CommandRegistry r;

    r.Register("PING", CommandSpec{.handler = CmdPing});
    r.Register("ECHO", CommandSpec{.handler = CmdEcho});
    r.Register("QUIT", CommandSpec{.handler = CmdQuit});
    r.Register("COMMAND", CommandSpec{.handler = CmdCommand});
    r.Register("CLIENT", CommandSpec{.handler = CmdClient});
    r.Register("HELLO", CommandSpec{.handler = CmdHello});
    r.Register("INFO", CommandSpec{.handler = CmdInfo});
    r.Register("CONFIG", CommandSpec{.handler = CmdConfig, .requires_cfg = true});

    r.Register("GET", CommandSpec{.handler = CmdGet, .requires_db = true});
    r.Register("SET", CommandSpec{.handler = CmdSet, .requires_db = true});
    r.Register("DEL", CommandSpec{.handler = CmdDel, .requires_db = true});
    r.Register("EXISTS", CommandSpec{.handler = CmdExists, .requires_db = true});
    r.Register("MGET", CommandSpec{.handler = CmdMget, .requires_db = true});
    r.Register("MSET", CommandSpec{.handler = CmdMset, .requires_db = true});
    r.Register("INCR", CommandSpec{.handler = CmdIncr, .requires_db = true});
    r.Register("DECR", CommandSpec{.handler = CmdDecr, .requires_db = true});
    r.Register("INCRBY", CommandSpec{.handler = CmdIncrBy, .requires_db = true});
    r.Register("DECRBY", CommandSpec{.handler = CmdDecrBy, .requires_db = true});
    r.Register("METRIC", CommandSpec{.handler = CmdMetric, .requires_db = true});
    r.Register("METRICS", CommandSpec{.handler = CmdMetric, .requires_db = true});
    r.Register("FLUSH", CommandSpec{.handler = CmdFlush, .requires_db = true});
    r.Register("COMPACT", CommandSpec{.handler = CmdCompact, .requires_db = true});
    r.Register("SCAN", CommandSpec{.handler = CmdScan, .requires_db = true});
    r.Register("SEEKTOFIRST", CommandSpec{.handler = CmdSeekToFirst, .requires_db = true});
    r.Register("SEEK", CommandSpec{.handler = CmdSeek, .requires_db = true});
    r.Register("VALID", CommandSpec{.handler = CmdValid, .requires_db = true});
    r.Register("NEXT", CommandSpec{.handler = CmdNext, .requires_db = true});
    r.Register("ITERDEL", CommandSpec{.handler = CmdIterDel});
    r.Register("HIDYLAN", CommandSpec{.handler = CmdHiDylan});

    return r;
  }();
  return reg;
}

}  // namespace

CommandResult ExecuteCommand(const std::vector<std::string>& args, dkv::DB* db, const ServerConfig* cfg) {
  CommandResult r;
  if (args.empty()) {
    resp::AppendError(r.payload, "ERR empty command");
    return r;
  }

  const std::string cmd = UpperAscii(args[0]);
  const CommandSpec* spec = DefaultRegistry().Lookup(cmd);
  if (!spec || !spec->handler) {
    resp::AppendError(r.payload, "ERR unknown command");
    return r;
  }

  if (spec->requires_cfg && !cfg) {
    resp::AppendError(r.payload, "ERR config not available");
    return r;
  }
  if (spec->requires_db && !db) {
    resp::AppendError(r.payload, "ERR db not ready");
    return r;
  }

  return spec->handler(args, db, cfg);
}

}  // namespace dkv_server
