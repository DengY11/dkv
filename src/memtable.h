#pragma once

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "dkv/status.h"

namespace dkv {

struct MemEntry {
  std::string key;
  std::string value;
  std::uint64_t seq{0};
  bool deleted{false};
};

struct MemEntryView {
  std::string_view key;
  std::string_view value;
  std::uint64_t seq{0};
  bool deleted{false};
};

// Lock-free (append-only) memtable using skip list and arena-backed storage.
// Nodes are never reclaimed until Clear()/destruction, making concurrent reads lock-free.
class MemTable {
 public:
  explicit MemTable(std::size_t approx_capacity_bytes = 0);
  ~MemTable();
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  Status Put(std::uint64_t seq, std::string_view key, std::string_view value);
  Status Delete(std::uint64_t seq, std::string_view key);
  bool Get(std::string_view key, MemEntry& entry) const;

  std::vector<MemEntry> Snapshot() const;
  std::vector<MemEntryView> SnapshotViews() const;
  void Clear();

  [[nodiscard]] std::size_t ApproximateMemoryUsage() const;
  [[nodiscard]] bool Empty() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dkv
