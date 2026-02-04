#pragma once

#include <charconv>
#include <cerrno>
#include <cstdint>
#include <initializer_list>
#include <optional>
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

  [[nodiscard]] bool IsSimpleString() const { return type == Type::kSimpleString; }
  [[nodiscard]] bool IsBulkString() const { return type == Type::kBulkString; }
  [[nodiscard]] bool IsInteger() const { return type == Type::kInteger; }
  [[nodiscard]] bool IsArray() const { return type == Type::kArray; }
  [[nodiscard]] bool IsString() const { return IsSimpleString() || IsBulkString(); }
  [[nodiscard]] bool IsNull() const { return type == Type::kNull; }
  [[nodiscard]] bool IsError() const { return type == Type::kError; }
  [[nodiscard]] bool IsOk() const { return IsSimpleString() && str == "OK"; }
  [[nodiscard]] std::string_view StringView() const { return str; }
  [[nodiscard]] std::int64_t Integer() const { return integer; }
};

class RespClient {
 public:
  using IterId = std::int64_t;

  struct IterState {
    bool valid{false};
    std::optional<std::string> key;
    std::optional<std::string> value;
  };

  struct ScanResult {
    IterId id{0};
    IterState state;
  };

  struct Raw {
    RespClient* c{nullptr};
    void Send(std::initializer_list<std::string_view> args) { c->SendCommand(args); }
    void Send(const std::vector<std::string_view>& args) { c->SendCommand(args); }
    RespValue Read() { return c->ReadReply(); }
    RespValue Call(std::initializer_list<std::string_view> args) { return c->Command(args); }
    RespValue Call(const std::vector<std::string_view>& args) { return c->Command(args); }
  };

  struct Basic {
    RespClient* c{nullptr};
    RespValue Ping() { return c->Ping(); }
    RespValue Ping(std::string_view message) { return c->Ping(message); }
    std::string PingText() { return std::string(RespClient::RequireStringView(c->Ping(), "PING")); }
    std::string PingText(std::string_view message) {
      return std::string(RespClient::RequireStringView(c->Ping(message), "PING"));
    }
    RespValue Echo(std::string_view message) { return c->Echo(message); }
    std::string EchoText(std::string_view message) {
      return std::string(RespClient::RequireStringView(c->Echo(message), "ECHO"));
    }
    RespValue Quit() { return c->Quit(); }
    bool QuitOk() { return RespClient::RequireOk(c->Quit(), "QUIT"); }
    RespValue Hello() { return c->Hello(); }
    RespValue Info() { return c->Info(); }
    std::string InfoText() { return std::string(RespClient::RequireStringView(c->Info(), "INFO")); }
    RespValue CommandList() { return c->CommandCommand(); }
    RespValue ClientSetInfo(std::string_view k, std::string_view v) { return c->ClientSetInfo(k, v); }
    bool ClientSetInfoOk(std::string_view k, std::string_view v) {
      return RespClient::RequireOk(c->ClientSetInfo(k, v), "CLIENT SETINFO");
    }
  };

  struct KV {
    RespClient* c{nullptr};
    RespValue Get(std::string_view key) { return c->Get(key); }
    RespValue Set(std::string_view key, std::string_view value) { return c->Set(key, value); }
    RespValue Del(std::initializer_list<std::string_view> keys) { return c->Del(keys); }
    RespValue Exists(std::initializer_list<std::string_view> keys) { return c->Exists(keys); }
    RespValue Mget(std::initializer_list<std::string_view> keys) { return c->Mget(keys); }
    RespValue Mset(std::initializer_list<std::pair<std::string_view, std::string_view>> kvs) { return c->Mset(kvs); }

    std::optional<std::string> GetString(std::string_view key) {
      return RespClient::OptionalString(c->Get(key), "GET");
    }
    bool SetOk(std::string_view key, std::string_view value) { return RespClient::RequireOk(c->Set(key, value), "SET"); }
    std::int64_t DelCount(std::initializer_list<std::string_view> keys) {
      return RespClient::RequireInt(c->Del(keys), "DEL");
    }
    std::int64_t ExistsCount(std::initializer_list<std::string_view> keys) {
      return RespClient::RequireInt(c->Exists(keys), "EXISTS");
    }
    std::vector<std::optional<std::string>> MgetStrings(std::initializer_list<std::string_view> keys) {
      RespValue v = c->Mget(keys);
      RespClient::RequireNotError(v, "MGET");
      if (!v.IsArray()) throw std::runtime_error("MGET: expected array");
      std::vector<std::optional<std::string>> out;
      out.reserve(v.array.size());
      for (const auto& item : v.array) {
        out.push_back(RespClient::OptionalString(item, "MGET"));
      }
      return out;
    }
    bool MsetOk(std::initializer_list<std::pair<std::string_view, std::string_view>> kvs) {
      return RespClient::RequireOk(c->Mset(kvs), "MSET");
    }
  };

  struct Counter {
    RespClient* c{nullptr};
    RespValue Incr(std::string_view key) { return c->Incr(key); }
    RespValue Decr(std::string_view key) { return c->Decr(key); }
    RespValue IncrBy(std::string_view key, std::int64_t by) { return c->IncrBy(key, by); }
    RespValue DecrBy(std::string_view key, std::int64_t by) { return c->DecrBy(key, by); }

    std::int64_t IncrValue(std::string_view key) { return RespClient::RequireInt(c->Incr(key), "INCR"); }
    std::int64_t DecrValue(std::string_view key) { return RespClient::RequireInt(c->Decr(key), "DECR"); }
    std::int64_t IncrByValue(std::string_view key, std::int64_t by) {
      return RespClient::RequireInt(c->IncrBy(key, by), "INCRBY");
    }
    std::int64_t DecrByValue(std::string_view key, std::int64_t by) {
      return RespClient::RequireInt(c->DecrBy(key, by), "DECRBY");
    }
  };

  struct Admin {
    RespClient* c{nullptr};
    RespValue Metrics() { return c->Metrics(); }
    RespValue Metric() { return c->Metric(); }
    RespValue Flush() { return c->Flush(); }
    RespValue Compact() { return c->Compact(); }

    std::vector<std::pair<std::string, std::string>> MetricsMap() {
      return RespClient::RequireKvArray(c->Metrics(), "METRICS");
    }
    std::vector<std::pair<std::string, std::string>> MetricMap() {
      return RespClient::RequireKvArray(c->Metric(), "METRIC");
    }
    bool FlushOk() { return RespClient::RequireOk(c->Flush(), "FLUSH"); }
    bool CompactOk() { return RespClient::RequireOk(c->Compact(), "COMPACT"); }
  };

  struct Config {
    RespClient* c{nullptr};
    RespValue Get(std::string_view pattern) { return c->ConfigGet(pattern); }
    RespValue ResetStat() { return c->ConfigResetStat(); }
    RespValue Rewrite() { return c->ConfigRewrite(); }

    std::vector<std::pair<std::string, std::string>> GetMap(std::string_view pattern) {
      return RespClient::RequireKvArray(c->ConfigGet(pattern), "CONFIG GET");
    }
    bool ResetStatOk() { return RespClient::RequireOk(c->ConfigResetStat(), "CONFIG RESETSTAT"); }
    bool RewriteOk() { return RespClient::RequireOk(c->ConfigRewrite(), "CONFIG REWRITE"); }
  };

  struct Iter {
    RespClient* c{nullptr};
    RespValue Scan(std::string_view prefix = {}) { return c->Scan(prefix); }
    RespValue SeekToFirst(std::string_view iter_id) { return c->SeekToFirst(iter_id); }
    RespValue Seek(std::string_view iter_id, std::string_view target) { return c->Seek(iter_id, target); }
    RespValue Valid(std::string_view iter_id) { return c->Valid(iter_id); }
    RespValue Next(std::string_view iter_id) { return c->Next(iter_id); }
    RespValue IterDel(std::string_view iter_id) { return c->IterDel(iter_id); }

    ScanResult ScanState(std::string_view prefix = {}) { return RespClient::ParseScan(c->Scan(prefix)); }
    IterState SeekToFirstState(IterId id) { return RespClient::ParseIterState(c->SeekToFirst(ToString(id))); }
    IterState SeekState(IterId id, std::string_view target) {
      return RespClient::ParseIterState(c->Seek(ToString(id), target));
    }
    IterState NextState(IterId id) { return RespClient::ParseIterState(c->Next(ToString(id))); }
    bool ValidBool(IterId id) { return RespClient::RequireInt(c->Valid(ToString(id)), "VALID") != 0; }
    bool IterDelOk(IterId id) { return RespClient::RequireOk(c->IterDel(ToString(id)), "ITERDEL"); }
  };

  struct Custom {
    RespClient* c{nullptr};
    RespValue HiDylan() { return c->HiDylan(); }
    std::string HiDylanText() { return std::string(RespClient::RequireStringView(c->HiDylan(), "HIDYLAN")); }
  };

  Raw raw() { return Raw{this}; }
  Basic basic() { return Basic{this}; }
  KV kv() { return KV{this}; }
  Counter counter() { return Counter{this}; }
  Admin admin() { return Admin{this}; }
  Config config() { return Config{this}; }
  Iter iter() { return Iter{this}; }
  Custom custom() { return Custom{this}; }

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

  RespValue Ping() { return Command({"PING"}); }
  RespValue Ping(std::string_view message) { return Command({"PING", message}); }
  RespValue Echo(std::string_view message) { return Command({"ECHO", message}); }
  RespValue Quit() { return Command({"QUIT"}); }
  RespValue Hello() { return Command({"HELLO"}); }
  RespValue Info() { return Command({"INFO"}); }
  RespValue CommandCommand() { return Command({"COMMAND"}); }
  RespValue ClientSetInfo(std::string_view k, std::string_view v) { return Command({"CLIENT", "SETINFO", k, v}); }

  RespValue Get(std::string_view key) { return Command({"GET", key}); }
  RespValue Set(std::string_view key, std::string_view value) { return Command({"SET", key, value}); }
  RespValue Del(std::initializer_list<std::string_view> keys) {
    std::vector<std::string_view> args;
    args.reserve(1 + keys.size());
    args.push_back("DEL");
    args.insert(args.end(), keys.begin(), keys.end());
    return Command(args);
  }
  RespValue Exists(std::initializer_list<std::string_view> keys) {
    std::vector<std::string_view> args;
    args.reserve(1 + keys.size());
    args.push_back("EXISTS");
    args.insert(args.end(), keys.begin(), keys.end());
    return Command(args);
  }
  RespValue Mget(std::initializer_list<std::string_view> keys) {
    std::vector<std::string_view> args;
    args.reserve(1 + keys.size());
    args.push_back("MGET");
    args.insert(args.end(), keys.begin(), keys.end());
    return Command(args);
  }
  RespValue Mset(std::initializer_list<std::pair<std::string_view, std::string_view>> kvs) {
    std::vector<std::string_view> args;
    args.reserve(1 + kvs.size() * 2);
    args.push_back("MSET");
    for (const auto& kv : kvs) {
      args.push_back(kv.first);
      args.push_back(kv.second);
    }
    return Command(args);
  }
  RespValue Incr(std::string_view key) { return Command({"INCR", key}); }
  RespValue Decr(std::string_view key) { return Command({"DECR", key}); }
  RespValue IncrBy(std::string_view key, std::int64_t by) { return Command({"INCRBY", key, ToString(by)}); }
  RespValue DecrBy(std::string_view key, std::int64_t by) { return Command({"DECRBY", key, ToString(by)}); }

  RespValue Metrics() { return Command({"METRICS"}); }
  RespValue Metric() { return Command({"METRIC"}); }
  RespValue Flush() { return Command({"FLUSH"}); }
  RespValue Compact() { return Command({"COMPACT"}); }

  RespValue ConfigGet(std::string_view pattern) { return Command({"CONFIG", "GET", pattern}); }
  RespValue ConfigResetStat() { return Command({"CONFIG", "RESETSTAT"}); }
  RespValue ConfigRewrite() { return Command({"CONFIG", "REWRITE"}); }

  RespValue Scan(std::string_view prefix = {}) {
    if (prefix.empty()) return Command({"SCAN"});
    return Command({"SCAN", prefix});
  }
  RespValue SeekToFirst(std::string_view iter_id) { return Command({"SEEKTOFIRST", iter_id}); }
  RespValue Seek(std::string_view iter_id, std::string_view target) { return Command({"SEEK", iter_id, target}); }
  RespValue Valid(std::string_view iter_id) { return Command({"VALID", iter_id}); }
  RespValue Next(std::string_view iter_id) { return Command({"NEXT", iter_id}); }
  RespValue IterDel(std::string_view iter_id) { return Command({"ITERDEL", iter_id}); }

  RespValue HiDylan() { return Command({"HIDYLAN"}); }

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
  static void RequireNotError(const RespValue& v, std::string_view ctx) {
    if (v.IsError()) throw std::runtime_error(std::string(ctx) + ": " + v.str);
  }

  static bool RequireOk(const RespValue& v, std::string_view ctx) {
    RequireNotError(v, ctx);
    return v.IsOk();
  }

  static std::string_view RequireStringView(const RespValue& v, std::string_view ctx) {
    RequireNotError(v, ctx);
    if (!v.IsString()) throw std::runtime_error(std::string(ctx) + ": expected string");
    return v.str;
  }

  static std::optional<std::string> OptionalString(const RespValue& v, std::string_view ctx) {
    RequireNotError(v, ctx);
    if (v.IsNull()) return std::nullopt;
    if (!v.IsString()) throw std::runtime_error(std::string(ctx) + ": expected string or null");
    return v.str;
  }

  static std::int64_t RequireInt(const RespValue& v, std::string_view ctx) {
    RequireNotError(v, ctx);
    if (!v.IsInteger()) throw std::runtime_error(std::string(ctx) + ": expected integer");
    return v.integer;
  }

  static std::vector<std::pair<std::string, std::string>> RequireKvArray(const RespValue& v, std::string_view ctx) {
    RequireNotError(v, ctx);
    if (!v.IsArray()) throw std::runtime_error(std::string(ctx) + ": expected array");
    if ((v.array.size() % 2) != 0) throw std::runtime_error(std::string(ctx) + ": expected even-sized array");
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(v.array.size() / 2);
    for (std::size_t i = 0; i < v.array.size(); i += 2) {
      auto key = RequireStringView(v.array[i], ctx);
      auto val = OptionalString(v.array[i + 1], ctx);
      out.emplace_back(std::string(key), val ? *val : std::string());
    }
    return out;
  }

  static IterState ParseIterState(const RespValue& v) {
    RequireNotError(v, "ITER");
    if (!v.IsArray() || v.array.size() != 3) throw std::runtime_error("ITER: expected array[3]");
    IterState out;
    out.valid = RequireInt(v.array[0], "ITER") != 0;
    out.key = OptionalString(v.array[1], "ITER");
    out.value = OptionalString(v.array[2], "ITER");
    return out;
  }

  static ScanResult ParseScan(const RespValue& v) {
    RequireNotError(v, "SCAN");
    if (!v.IsArray() || v.array.size() != 4) throw std::runtime_error("SCAN: expected array[4]");
    ScanResult out;
    out.id = RequireInt(v.array[0], "SCAN");
    out.state.valid = RequireInt(v.array[1], "SCAN") != 0;
    out.state.key = OptionalString(v.array[2], "SCAN");
    out.state.value = OptionalString(v.array[3], "SCAN");
    return out;
  }

  static void AppendNumber(std::string& out, std::int64_t value) {
    char buf[64];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    (void)ec;
    out.append(buf, static_cast<std::size_t>(ptr - buf));
  }

  static std::string ToString(std::int64_t value) {
    char buf[64];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    (void)ec;
    return std::string(buf, static_cast<std::size_t>(ptr - buf));
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
