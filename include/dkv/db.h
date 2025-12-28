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

  class Iterator;

  static Status Open(const Options& options, std::unique_ptr<DB>& db);
  ~DB();

  Status Put(const WriteOptions& options, std::string key, std::string value);
  Status Delete(const WriteOptions& options, std::string key);
  // Applies batch as a single WAL sync and memtable update under lock.
  Status Write(const WriteOptions& options, const WriteBatch& batch);
  Status Get(const ReadOptions& options, std::string_view key, std::string& value);
  Status Flush();
  Status Compact();

  // supports Seek/Next over the latest visible view (or a snapshot if enabled in ReadOptions).
  std::unique_ptr<Iterator> Scan(const ReadOptions& options, std::string_view prefix = {});

  Metrics GetMetrics() const;

 private:
  explicit DB(Options options);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

class DB::Iterator {
 public:
  Iterator(Iterator&&) noexcept;
  Iterator& operator=(Iterator&&) noexcept;
  ~Iterator();

  void SeekToFirst();
  void Seek(std::string_view target);
  bool Valid() const;
  void Next();
  std::string_view key() const;
  std::string_view value() const;
  Status status() const;
  std::string_view prefix() const;

 private:
  struct Rep;
  explicit Iterator(std::unique_ptr<Rep> rep);
  std::unique_ptr<Rep> rep_;
  friend class DB;
  friend class DB::Impl;
};

}  // namespace dkv
