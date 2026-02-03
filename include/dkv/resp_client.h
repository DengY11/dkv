#pragma once

#include <charconv>
#include <cerrno>
#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace dkv {

struct RespValue {
  enum class Type { kSimpleString, kError, kInteger, kBulkString, kArray, kNull };
  Type type{Type::kNull};
  std::string str;
  std::int64_t integer{0};
  std::vector<RespValue> array;

  [[nodiscard]] bool IsNull() const { return type == Type::kNull; }
  [[nodiscard]] bool IsError() const { return type == Type::kError; }
};

class RespClient {
 public:
  RespClient() = default;
  RespClient(std::string_view host, std::uint16_t port) { Connect(host, port); }
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(std::exchange(other.fd_, -1)), inbuf_(std::move(other.inbuf_)) {}
  RespClient& operator=(RespClient&& other) noexcept {
    if (this == &other) return *this;
    Close();
    fd_ = std::exchange(other.fd_, -1);
    inbuf_ = std::move(other.inbuf_);
    return *this;
  }
  ~RespClient() { Close(); }

  void Connect(std::string_view host, std::uint16_t port) {
    Close();

    std::string host_str(host);
    std::string port_str = std::to_string(port);

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;

    addrinfo* result = nullptr;
    int rc = ::getaddrinfo(host_str.c_str(), port_str.c_str(), &hints, &result);
    if (rc != 0) {
      throw std::runtime_error(std::string("getaddrinfo: ") + ::gai_strerror(rc));
    }

    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
      int fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
        fd_ = fd;
        break;
      }
      ::close(fd);
    }

    ::freeaddrinfo(result);
    if (fd_ < 0) {
      throw std::runtime_error("connect failed");
    }
  }

  void Close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    inbuf_.clear();
  }

  [[nodiscard]] bool Connected() const { return fd_ >= 0; }

  void SendCommand(std::initializer_list<std::string_view> args) { SendAll(BuildCommand(args)); }
  void SendCommand(const std::vector<std::string_view>& args) { SendAll(BuildCommand(args)); }

  RespValue ReadReply() { return ReadValue(); }

  RespValue Command(std::initializer_list<std::string_view> args) {
    SendCommand(args);
    return ReadReply();
  }

  RespValue Command(const std::vector<std::string_view>& args) {
    SendCommand(args);
    return ReadReply();
  }

  static std::string BuildCommand(std::initializer_list<std::string_view> args) {
    return BuildCommand(std::vector<std::string_view>(args.begin(), args.end()));
  }

  static std::string BuildCommand(const std::vector<std::string_view>& args) {
    std::string out;
    out.reserve(64);
    out.push_back('*');
    AppendNumber(out, static_cast<std::int64_t>(args.size()));
    out.append("\r\n", 2);
    for (const auto& arg : args) {
      out.push_back('$');
      AppendNumber(out, static_cast<std::int64_t>(arg.size()));
      out.append("\r\n", 2);
      out.append(arg.data(), arg.size());
      out.append("\r\n", 2);
    }
    return out;
  }

 private:
  static void AppendNumber(std::string& out, std::int64_t value) {
    char buf[64];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    (void)ec;
    out.append(buf, static_cast<std::size_t>(ptr - buf));
  }

  [[nodiscard]] static std::int64_t ParseNumber(std::string_view text) {
    std::int64_t value = 0;
    auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size()) {
      throw std::runtime_error("invalid RESP number");
    }
    return value;
  }

  void EnsureConnected() const {
    if (fd_ < 0) throw std::runtime_error("RESP client not connected");
  }

  void SendAll(std::string_view data) {
    EnsureConnected();
    const char* ptr = data.data();
    std::size_t remaining = data.size();
    while (remaining > 0) {
      ssize_t sent = ::send(fd_, ptr, remaining, 0);
      if (sent < 0) {
        if (errno == EINTR) continue;
        ThrowSys("send");
      }
      ptr += sent;
      remaining -= static_cast<std::size_t>(sent);
    }
  }

  RespValue ReadValue() {
    std::string line = ReadLine();
    if (line.empty()) throw std::runtime_error("empty RESP reply");
    const char type = line[0];
    std::string_view payload(line.data() + 1, line.size() - 1);
    switch (type) {
      case '+': {
        RespValue v;
        v.type = RespValue::Type::kSimpleString;
        v.str.assign(payload.data(), payload.size());
        return v;
      }
      case '-': {
        RespValue v;
        v.type = RespValue::Type::kError;
        v.str.assign(payload.data(), payload.size());
        return v;
      }
      case ':': {
        RespValue v;
        v.type = RespValue::Type::kInteger;
        v.integer = ParseNumber(payload);
        return v;
      }
      case '$': {
        const std::int64_t len = ParseNumber(payload);
        if (len < 0) return RespValue{};
        RespValue v;
        v.type = RespValue::Type::kBulkString;
        v.str = ReadBytes(static_cast<std::size_t>(len));
        Consume(2);
        return v;
      }
      case '*': {
        const std::int64_t count = ParseNumber(payload);
        if (count < 0) return RespValue{};
        RespValue v;
        v.type = RespValue::Type::kArray;
        v.array.reserve(static_cast<std::size_t>(count));
        for (std::int64_t i = 0; i < count; ++i) {
          v.array.push_back(ReadValue());
        }
        return v;
      }
      default:
        throw std::runtime_error("unknown RESP reply type");
    }
  }

  std::string ReadLine() {
    EnsureConnected();
    for (;;) {
      std::size_t pos = inbuf_.find("\r\n");
      if (pos != std::string::npos) {
        std::string line = inbuf_.substr(0, pos);
        inbuf_.erase(0, pos + 2);
        return line;
      }
      ReadIntoBuffer();
    }
  }

  std::string ReadBytes(std::size_t n) {
    EnsureConnected();
    while (inbuf_.size() < n) {
      ReadIntoBuffer();
    }
    std::string out = inbuf_.substr(0, n);
    inbuf_.erase(0, n);
    return out;
  }

  void Consume(std::size_t n) {
    EnsureConnected();
    while (inbuf_.size() < n) {
      ReadIntoBuffer();
    }
    inbuf_.erase(0, n);
  }

  void ReadIntoBuffer() {
    EnsureConnected();
    char buf[4096];
    ssize_t received = 0;
    for (;;) {
      received = ::recv(fd_, buf, sizeof(buf), 0);
      if (received < 0 && errno == EINTR) continue;
      break;
    }
    if (received < 0) ThrowSys("recv");
    if (received == 0) throw std::runtime_error("connection closed");
    inbuf_.append(buf, static_cast<std::size_t>(received));
  }

  static void ThrowSys(const char* what) { throw std::system_error(errno, std::generic_category(), what); }

  int fd_{-1};
  std::string inbuf_;
};

}  // namespace dkv
