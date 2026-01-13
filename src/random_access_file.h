#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "dkv/status.h"

namespace dkv {

class RandomAccessFile {
 public:
  RandomAccessFile() = default;
  ~RandomAccessFile();

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  RandomAccessFile(RandomAccessFile&& other) noexcept;
  RandomAccessFile& operator=(RandomAccessFile&& other) noexcept;

  Status Open(const std::filesystem::path& path);
  void Close();

  [[nodiscard]] bool Valid() const;

  // Reads exactly `n` bytes from `offset` into `dst`.
  Status Read(std::uint64_t offset, std::size_t n, char* dst) const;

 private:
#if defined(_WIN32)
  void* handle_{reinterpret_cast<void*>(-1)};  // INVALID_HANDLE_VALUE
#else
  int fd_{-1};
#endif
  std::filesystem::path path_;
};

}  // namespace dkv

