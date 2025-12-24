#include "bloom.h"

#include <algorithm>
#include <cmath>

namespace dkv {

std::uint64_t BloomFilter::Hash64(std::string_view key) {
  // FNV-1a 64-bit
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (unsigned char c : key) {
    hash ^= static_cast<std::uint64_t>(c);
    hash *= 0x100000001b3ULL;
  }
  return hash;
}

BloomFilter BloomFilter::Build(const std::vector<std::string>& keys, std::uint32_t bits_per_key) {
  BloomFilter f;
  if (keys.empty() || bits_per_key == 0) return f;

  const std::uint32_t k = std::max<std::uint32_t>(1, static_cast<std::uint32_t>(bits_per_key * 0.69));
  const std::uint32_t base_bits = static_cast<std::uint32_t>(keys.size() * bits_per_key);
  std::vector<std::uint8_t> bits((base_bits + 7) / 8, 0);
  const std::uint32_t m = static_cast<std::uint32_t>(bits.size() * 8);

  for (const auto& key : keys) {
    auto h = Hash64(key);
    const std::uint32_t delta = static_cast<std::uint32_t>((h >> 17) | (h << 15));
    for (std::uint32_t i = 0; i < k; ++i) {
      const std::uint32_t bitpos = static_cast<std::uint32_t>((h + i * delta) % m);
      bits[bitpos / 8] |= static_cast<std::uint8_t>(1u << (bitpos % 8));
    }
  }

  f.bits_ = std::move(bits);
  f.bits_per_key_ = bits_per_key;
  f.k_ = k;
  f.bit_size_ = m;
  return f;
}

bool BloomFilter::MayContain(std::string_view key) const {
  if (bits_.empty()) return true;
  const std::uint32_t m = bit_size_;
  auto h = Hash64(key);
  const std::uint32_t delta = static_cast<std::uint32_t>((h >> 17) | (h << 15));
  for (std::uint32_t i = 0; i < k_; ++i) {
    const std::uint32_t bitpos = static_cast<std::uint32_t>((h + i * delta) % m);
    if ((bits_[bitpos / 8] & (1u << (bitpos % 8))) == 0) return false;
  }
  return true;
}

}  // namespace dkv
