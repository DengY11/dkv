#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dkv/options.h"
#include "dkv/status.h"

namespace dkv {

struct BatchOp {
  enum class Type { kPut, kDelete };
  Type type;
  std::string key;
  std::string value;
};

class WriteBatch {
 public:
  void Put(std::string key, std::string value) {
    ops_.push_back(BatchOp{BatchOp::Type::kPut, std::move(key), std::move(value)});
  }
  void Delete(std::string key) { ops_.push_back(BatchOp{BatchOp::Type::kDelete, std::move(key), {}}); }
  void Clear() { ops_.clear(); }
  [[nodiscard]] const std::vector<BatchOp>& ops() const { return ops_; }
  [[nodiscard]] bool empty() const { return ops_.empty(); }

 private:
  std::vector<BatchOp> ops_;
};

struct Metrics {
  std::uint64_t puts{0};
  std::uint64_t deletes{0};
  std::uint64_t gets{0};
  std::uint64_t batches{0};

  std::uint64_t flushes{0};
  std::uint64_t flush_ms{0};
  std::uint64_t flush_bytes{0};

  std::uint64_t compactions{0};
  std::uint64_t compaction_ms{0};
  std::uint64_t compaction_input_bytes{0};
  std::uint64_t compaction_output_bytes{0};

  std::uint64_t wal_syncs{0};
};

class DB {
 public:
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  static Status Open(const Options& options, std::unique_ptr<DB>& db);
  ~DB();

  Status Put(const WriteOptions& options, std::string key, std::string value);
  Status Delete(const WriteOptions& options, std::string key);
  // Applies batch as a single WAL sync and memtable update under lock.
  Status Write(const WriteOptions& options, const WriteBatch& batch);
  Status Get(const ReadOptions& options, std::string_view key, std::string& value);
  Status Flush();
  Status Compact();

  // Collects up to limit sorted key/value pairs starting at "from" (inclusive).
  Status Scan(const ReadOptions& options, std::string_view from, std::size_t limit,
              std::vector<std::pair<std::string, std::string>>& out);
  Metrics GetMetrics() const;

 private:
  explicit DB(Options options);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dkv
