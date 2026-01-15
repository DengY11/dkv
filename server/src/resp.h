#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dkv_server::resp {

enum class ParseResult { kOk, kNeedMore, kError };

ParseResult ParseCommand(std::string_view input, std::size_t* consumed, std::vector<std::string>* out_args,
                         std::string* out_error);

inline void AppendSimpleString(std::string& out, std::string_view s) {
  out.append("+", 1);
  out.append(s);
  out.append("\r\n", 2);
}

inline void AppendError(std::string& out, std::string_view s) {
  out.append("-", 1);
  out.append(s);
  out.append("\r\n", 2);
}

inline void AppendInteger(std::string& out, std::int64_t v) {
  out.append(":", 1);
  char buf[64];
  auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
  (void)ec;
  out.append(buf, static_cast<std::size_t>(ptr - buf));
  out.append("\r\n", 2);
}

inline void AppendNullBulkString(std::string& out) { out.append("$-1\r\n", 5); }

inline void AppendBulkString(std::string& out, std::string_view s) {
  out.append("$", 1);
  char buf[64];
  auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), s.size());
  (void)ec;
  out.append(buf, static_cast<std::size_t>(ptr - buf));
  out.append("\r\n", 2);
  out.append(s);
  out.append("\r\n", 2);
}

inline void AppendArrayHeader(std::string& out, std::size_t n) {
  out.append("*", 1);
  char buf[64];
  auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), n);
  (void)ec;
  out.append(buf, static_cast<std::size_t>(ptr - buf));
  out.append("\r\n", 2);
}

}  // namespace dkv_server::resp

