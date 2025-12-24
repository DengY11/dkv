#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string_view>

#include "dkv/status.h"

namespace dkv {

constexpr std::uint32_t kSSTableMagic = 0xD15EEDu;
constexpr std::size_t kSSTableFooterSize = sizeof(std::uint64_t) * 3 + sizeof(std::uint32_t) * 2;

inline void WriteU8(std::ostream& os, std::uint8_t value) { os.write(reinterpret_cast<const char*>(&value), 1); }

inline void WriteU32(std::ostream& os, std::uint32_t value) {
  unsigned char buf[4];
  buf[0] = static_cast<unsigned char>(value & 0xFFu);
  buf[1] = static_cast<unsigned char>((value >> 8u) & 0xFFu);
  buf[2] = static_cast<unsigned char>((value >> 16u) & 0xFFu);
  buf[3] = static_cast<unsigned char>((value >> 24u) & 0xFFu);
  os.write(reinterpret_cast<const char*>(buf), 4);
}

inline void WriteU64(std::ostream& os, std::uint64_t value) {
  unsigned char buf[8];
  for (int i = 0; i < 8; ++i) {
    buf[i] = static_cast<unsigned char>((value >> (i * 8)) & 0xFFu);
  }
  os.write(reinterpret_cast<const char*>(buf), 8);
}

inline bool ReadU8(std::istream& is, std::uint8_t& value) {
  unsigned char buf = 0;
  if (!is.read(reinterpret_cast<char*>(&buf), 1)) return false;
  value = static_cast<std::uint8_t>(buf);
  return true;
}

inline bool ReadU32(std::istream& is, std::uint32_t& value) {
  unsigned char buf[4]{0, 0, 0, 0};
  if (!is.read(reinterpret_cast<char*>(buf), 4)) return false;
  value = static_cast<std::uint32_t>(buf[0]) | (static_cast<std::uint32_t>(buf[1]) << 8u) |
          (static_cast<std::uint32_t>(buf[2]) << 16u) | (static_cast<std::uint32_t>(buf[3]) << 24u);
  return true;
}

inline bool ReadU64(std::istream& is, std::uint64_t& value) {
  unsigned char buf[8]{0};
  if (!is.read(reinterpret_cast<char*>(buf), 8)) return false;
  value = 0;
  for (int i = 7; i >= 0; --i) {
    value <<= 8u;
    value |= static_cast<std::uint64_t>(buf[i]);
  }
  return true;
}

std::optional<std::uint64_t> FileSize(const std::filesystem::path& path);
Status SyncFileToDisk(const std::filesystem::path& path);
Status SyncParentDir(const std::filesystem::path& path);
std::uint32_t CRC32(std::string_view data);

}  // namespace dkv
