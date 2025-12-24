#pragma once

#include <string>

namespace dkv {

class Status {
 public:
  enum class Code {
    kOk = 0,
    kNotFound,
    kIOError,
    kInvalidArgument,
    kCorruption,
  };

  Status() : code_(Code::kOk) {}
  Status(Code code, std::string message) : code_(code), message_(std::move(message)) {}

  static Status OK() { return Status(); }
  static Status NotFound(std::string message) { return Status(Code::kNotFound, std::move(message)); }
  static Status IOError(std::string message) { return Status(Code::kIOError, std::move(message)); }
  static Status InvalidArgument(std::string message) {
    return Status(Code::kInvalidArgument, std::move(message));
  }
  static Status Corruption(std::string message) { return Status(Code::kCorruption, std::move(message)); }

  [[nodiscard]] bool ok() const { return code_ == Code::kOk; }
  [[nodiscard]] Code code() const { return code_; }
  [[nodiscard]] const std::string& message() const { return message_; }
  [[nodiscard]] std::string ToString() const;

 private:
  Code code_;
  std::string message_;
};

}  // namespace dkv
