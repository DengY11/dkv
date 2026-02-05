#include "log.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace dkv_server {
namespace {

const char* ToString(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
  }
  return "INFO";
}

std::string NowString() {
  auto now = std::chrono::system_clock::now();
  auto t = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  localtime_s(&tm, &t);
#else
  localtime_r(&t, &tm);
#endif
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.'
      << std::setw(3) << std::setfill('0') << ms.count();
  return oss.str();
}

}  // namespace

AsyncLogger& AsyncLogger::Instance() {
  static AsyncLogger logger;
  return logger;
}

AsyncLogger::~AsyncLogger() { Stop(); }

void AsyncLogger::Start() {
  std::lock_guard lk(mu_);
  if (running_) return;
  stop_ = false;
  running_ = true;
  thread_ = std::thread([this] { Run(); });
}

void AsyncLogger::Stop() {
  {
    std::lock_guard lk(mu_);
    if (!running_) return;
    stop_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
  running_ = false;
}

void AsyncLogger::SetLevel(LogLevel level) { level_.store(level, std::memory_order_relaxed); }

void AsyncLogger::Log(LogLevel level, std::string_view msg) {
  if (static_cast<int>(level) < static_cast<int>(level_.load(std::memory_order_relaxed))) return;
  std::lock_guard lk(mu_);
  if (!running_) {
    std::ostream& os = (level >= LogLevel::kWarn) ? std::cerr : std::cout;
    os << msg << '\n';
    return;
  }
  queue_.push_back(Entry{level, std::string(msg)});
  cv_.notify_one();
}

void AsyncLogger::Run() {
  for (;;) {
    Entry entry;
    {
      std::unique_lock lk(mu_);
      cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
      if (stop_ && queue_.empty()) break;
      entry = std::move(queue_.front());
      queue_.pop_front();
    }
    std::ostream& os = (entry.level >= LogLevel::kWarn) ? std::cerr : std::cout;
    os << NowString() << " [" << ToString(entry.level) << "] " << entry.msg << '\n';
  }
}

}  // namespace dkv_server
