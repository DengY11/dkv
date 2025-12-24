#pragma once

#include <cstddef>
#include <functional>
#include <map>
#include <shared_mutex>
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

class MemTable {
 public:
  Status Put(std::uint64_t seq, std::string key, std::string value);
  Status Delete(std::uint64_t seq, std::string key);
  bool Get(std::string_view key, MemEntry& entry) const;

  std::vector<MemEntry> Snapshot() const;
  void Clear();

  [[nodiscard]] std::size_t ApproximateMemoryUsage() const;
  [[nodiscard]] bool Empty() const;

 private:
  struct MemValue {
    //dont use MemEntry here because key is unnecessary
    std::string value;
    std::uint64_t seq{0};
    bool deleted{false};
  };

  mutable std::shared_mutex mu_;
  std::map<std::string, MemValue, std::less<>> table_;
  std::size_t memory_usage_{0};
};

}  // namespace dkv
