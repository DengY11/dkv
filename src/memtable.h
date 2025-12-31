#pragma once

#include <cstddef>
#include <array>
#include <functional>
#include <map>
#include <memory>
#include <memory_resource>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
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

class MemTable {
 public:
  explicit MemTable(std::size_t shard_count = 16, std::size_t approx_capacity_bytes = 0);
  ~MemTable();
  Status Put(std::uint64_t seq, std::string_view key, std::string_view value);
  Status Delete(std::uint64_t seq, std::string_view key);
  bool Get(std::string_view key, MemEntry& entry) const;

  std::vector<MemEntry> Snapshot() const;
  std::vector<MemEntryView> SnapshotViews() const;
  void Clear();

  [[nodiscard]] std::size_t ApproximateMemoryUsage() const;
  [[nodiscard]] bool Empty() const;

 private:
  struct MemValue {
    // dont use MemEntry here because key is unnecessary
    std::pmr::string value;
    std::uint64_t seq{0};
    bool deleted{false};
  };

  struct Shard;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::size_t shard_count_{16};
  std::size_t base_usage_{0};

  [[nodiscard]] std::size_t RawMemoryUsage() const;
};

}  // namespace dkv
