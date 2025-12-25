#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dkv {

class BloomFilter {
 public:
  BloomFilter() = default;

  static BloomFilter Build(const std::vector<std::string>& keys, std::uint32_t bits_per_key);
  static BloomFilter Build(const std::vector<std::string_view>& keys, std::uint32_t bits_per_key);

  [[nodiscard]] bool MayContain(std::string_view key) const;
  [[nodiscard]] const std::vector<std::uint8_t>& data() const { return bits_; }
  [[nodiscard]] std::uint32_t bits_per_key() const { return bits_per_key_; }
  [[nodiscard]] std::uint32_t hash_count() const { return k_; }
  [[nodiscard]] bool empty() const { return bits_.empty(); }
  [[nodiscard]] std::uint32_t bit_size() const { return bit_size_; }

  void SetData(std::vector<std::uint8_t> data, std::uint32_t bits_per_key, std::uint32_t k) {
    bits_ = std::move(data);
    bits_per_key_ = bits_per_key;
    k_ = k;
    bit_size_ = static_cast<std::uint32_t>(bits_.size() * 8);
  }

 private:
  static std::uint64_t Hash64(std::string_view key);

  std::vector<std::uint8_t> bits_;
  std::uint32_t bits_per_key_{0};
  std::uint32_t k_{0};
  std::uint32_t bit_size_{0};
};

}  // namespace dkv
