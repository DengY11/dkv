#include "util.h"

#include <cerrno>
#include <system_error>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace dkv {

std::optional<std::uint64_t> FileSize(const std::filesystem::path& path) {
  std::error_code ec;
  auto size = std::filesystem::file_size(path, ec);
  if (ec) return std::nullopt;
  return size;
}

Status SyncFileToDisk(const std::filesystem::path& path) {
#ifdef _WIN32
  int fd = _open(path.string().c_str(), _O_BINARY | _O_RDWR);
  if (fd < 0) return Status::IOError("open for sync failed: " + path.string());
  int rc = _commit(fd);
  _close(fd);
  if (rc != 0) return Status::IOError("fsync failed: " + path.string());
#else
  int fd = ::open(path.c_str(), O_RDWR);
  if (fd < 0) return Status::IOError("open for sync failed: " + path.string());
  int rc = ::fsync(fd);
  ::close(fd);
  if (rc != 0) return Status::IOError("fsync failed: " + path.string());
#endif
  return Status::OK();
}

}  // namespace dkv
