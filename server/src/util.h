#pragma once

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace dkv_server {

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this == &other) return *this;
    reset(std::exchange(other.fd_, -1));
    return *this;
  }
  ~UniqueFd() { reset(); }

  [[nodiscard]] int get() const { return fd_; }
  [[nodiscard]] bool valid() const { return fd_ >= 0; }

  int release() { return std::exchange(fd_, -1); }

  void reset(int fd = -1) {
    if (fd_ >= 0) ::close(fd_);
    fd_ = fd;
  }

 private:
  int fd_{-1};
};

[[nodiscard]] inline std::string ErrnoString(int err) { return std::string(std::strerror(err)); }

inline void ThrowSys(const char* what) { throw std::system_error(errno, std::generic_category(), what); }

inline void SetNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) ThrowSys("fcntl(F_GETFL)");
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) ThrowSys("fcntl(F_SETFL)");
}

inline void SetReuseAddr(int fd) {
  int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) ThrowSys("setsockopt(SO_REUSEADDR)");
}

inline void SetTcpNoDelay(int fd) {
  int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) ThrowSys("setsockopt(TCP_NODELAY)");
}

inline void SetKeepAlive(int fd) {
  int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0) ThrowSys("setsockopt(SO_KEEPALIVE)");
}

}  // namespace dkv_server

