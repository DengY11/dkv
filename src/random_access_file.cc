#include "random_access_file.h"

#include <cerrno>
#include <cstring>
#include <string>

#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace dkv {

RandomAccessFile::~RandomAccessFile() { Close(); }

RandomAccessFile::RandomAccessFile(RandomAccessFile&& other) noexcept { *this = std::move(other); }

RandomAccessFile& RandomAccessFile::operator=(RandomAccessFile&& other) noexcept {
  if (this == &other) return *this;
  Close();
#if defined(_WIN32)
  handle_ = other.handle_;
  other.handle_ = reinterpret_cast<void*>(-1);
#else
  fd_ = other.fd_;
  other.fd_ = -1;
#endif
  path_ = std::move(other.path_);
  return *this;
}

Status RandomAccessFile::Open(const std::filesystem::path& path) {
  Close();
  path_ = path;
#if defined(_WIN32)
  const std::wstring wpath = path.wstring();
  HANDLE h = ::CreateFileW(wpath.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                           FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    const DWORD err = ::GetLastError();
    return Status::IOError("CreateFile failed: " + path.string() + " (err=" + std::to_string(err) + ")");
  }
  handle_ = h;
  return Status::OK();
#else
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("open failed: " + path.string() + ": " + std::strerror(errno));
  }
  fd_ = fd;
  return Status::OK();
#endif
}

void RandomAccessFile::Close() {
#if defined(_WIN32)
  if (handle_ != reinterpret_cast<void*>(-1)) {
    ::CloseHandle(reinterpret_cast<HANDLE>(handle_));
    handle_ = reinterpret_cast<void*>(-1);
  }
#else
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
#endif
}

bool RandomAccessFile::Valid() const {
#if defined(_WIN32)
  return handle_ != reinterpret_cast<void*>(-1);
#else
  return fd_ >= 0;
#endif
}

Status RandomAccessFile::Read(std::uint64_t offset, std::size_t n, char* dst) const {
  if (!Valid()) return Status::IOError("file not open: " + path_.string());
  std::size_t done = 0;
  while (done < n) {
#if defined(_WIN32)
    OVERLAPPED ov{};
    const std::uint64_t off = offset + done;
    ov.Offset = static_cast<DWORD>(off & 0xFFFFFFFFu);
    ov.OffsetHigh = static_cast<DWORD>((off >> 32u) & 0xFFFFFFFFu);
    DWORD read = 0;
    if (!::ReadFile(reinterpret_cast<HANDLE>(handle_), dst + done, static_cast<DWORD>(n - done), &read, &ov)) {
      const DWORD err = ::GetLastError();
      return Status::IOError("ReadFile failed: " + path_.string() + " (err=" + std::to_string(err) + ")");
    }
    if (read == 0) {
      return Status::IOError("short read: " + path_.string());
    }
    done += static_cast<std::size_t>(read);
#else
    const off_t off = static_cast<off_t>(offset + done);
    const ssize_t r = ::pread(fd_, dst + done, n - done, off);
    if (r == 0) return Status::IOError("short read: " + path_.string());
    if (r < 0) {
      if (errno == EINTR) continue;
      return Status::IOError("pread failed: " + path_.string() + ": " + std::strerror(errno));
    }
    done += static_cast<std::size_t>(r);
#endif
  }
  return Status::OK();
}

}  // namespace dkv

