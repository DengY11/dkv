#pragma once

#include <cstddef>
#include <array>
#include <functional>
#include <map>
#include <memory_resource>
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
  Status Put(std::uint64_t seq, std::string_view key, std::string_view value);
  Status Delete(std::uint64_t seq, std::string_view key);
  bool Get(std::string_view key, MemEntry& entry) const;

  std::vector<MemEntry> Snapshot() const;
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

  mutable std::shared_mutex mu_;
  static constexpr std::size_t kInlineArenaSize = 64 * 1024;
  alignas(std::max_align_t) std::array<std::byte, kInlineArenaSize> inline_arena_;
  std::pmr::monotonic_buffer_resource buffer_{inline_arena_.data(), inline_arena_.size()};
  std::pmr::map<std::pmr::string, MemValue, std::less<>> table_{&buffer_};
  std::size_t memory_usage_{0};
};

}  // namespace dkv
