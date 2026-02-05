#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace dkv_server {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

class AsyncLogger {
 public:
  static AsyncLogger& Instance();

  void Start();
  void Stop();
  void SetLevel(LogLevel level);
  void Log(LogLevel level, std::string_view msg);

 private:
  struct Entry {
    LogLevel level{LogLevel::kInfo};
    std::string msg;
  };

  AsyncLogger() = default;
  ~AsyncLogger();
  AsyncLogger(const AsyncLogger&) = delete;
  AsyncLogger& operator=(const AsyncLogger&) = delete;

  void Run();

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Entry> queue_;
  std::thread thread_;
  std::atomic<LogLevel> level_{LogLevel::kInfo};
  bool running_{false};
  bool stop_{false};
};

inline void LogDebug(std::string_view msg) { AsyncLogger::Instance().Log(LogLevel::kDebug, msg); }
inline void LogInfo(std::string_view msg) { AsyncLogger::Instance().Log(LogLevel::kInfo, msg); }
inline void LogWarn(std::string_view msg) { AsyncLogger::Instance().Log(LogLevel::kWarn, msg); }
inline void LogError(std::string_view msg) { AsyncLogger::Instance().Log(LogLevel::kError, msg); }

}  // namespace dkv_server
