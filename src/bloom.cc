#include "bloom.h"

#include <algorithm>
#include <cmath>

namespace dkv {

namespace {

std::uint64_t Hash64Impl(std::string_view key) {
  // FNV-1a 64-bit
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (unsigned char c : key) {
    hash ^= static_cast<std::uint64_t>(c);
    hash *= 0x100000001b3ULL;
  }
  return hash;
}

}  // namespace

std::uint64_t BloomFilter::Hash64(std::string_view key) {
  return Hash64Impl(key);
}

namespace {

template <typename Range>
void FillBits(const Range& keys, std::uint32_t bits_per_key, BloomFilter& out) {
  if (keys.empty() || bits_per_key == 0) return;

  const std::uint32_t k = std::max<std::uint32_t>(1, static_cast<std::uint32_t>(bits_per_key * 0.69));
  const std::uint32_t base_bits = static_cast<std::uint32_t>(keys.size() * bits_per_key);
  std::vector<std::uint8_t> bits((base_bits + 7) / 8, 0);
  const std::uint32_t m = static_cast<std::uint32_t>(bits.size() * 8);

  for (const auto& key : keys) {
    auto h = Hash64Impl(std::string_view(key));
    const std::uint32_t delta = static_cast<std::uint32_t>((h >> 17) | (h << 15));
    for (std::uint32_t i = 0; i < k; ++i) {
      const std::uint32_t bitpos = static_cast<std::uint32_t>((h + i * delta) % m);
      bits[bitpos / 8] |= static_cast<std::uint8_t>(1u << (bitpos % 8));
    }
  }

  out.SetData(std::move(bits), bits_per_key, k);
}

}  // namespace

BloomFilter BloomFilter::Build(const std::vector<std::string>& keys, std::uint32_t bits_per_key) {
  BloomFilter f;
  FillBits(keys, bits_per_key, f);
  return f;
}

BloomFilter BloomFilter::Build(const std::vector<std::string_view>& keys, std::uint32_t bits_per_key) {
  BloomFilter f;
  FillBits(keys, bits_per_key, f);
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
