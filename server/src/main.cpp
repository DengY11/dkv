#include <atomic>
#include <charconv>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <execinfo.h>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

#include "server.h"

namespace {
std::atomic_bool g_stop{false};

void OnSignal(int) { g_stop.store(true, std::memory_order_relaxed); }

void OnFatalSignal(int sig) {
  const char* name = "unknown";
  switch (sig) {
    case SIGSEGV:
      name = "SIGSEGV";
      break;
    case SIGABRT:
      name = "SIGABRT";
      break;
    case SIGBUS:
      name = "SIGBUS";
      break;
    case SIGILL:
      name = "SIGILL";
      break;
    default:
      break;
  }
  std::cerr << "[fatal] signal " << name << " (" << sig << ")\n";

  void* frames[64];
  int n = ::backtrace(frames, static_cast<int>(sizeof(frames) / sizeof(frames[0])));
  if (n > 0) {
    ::backtrace_symbols_fd(frames, n, STDERR_FILENO);
  }
  std::_Exit(128 + sig);
}

std::string LowerAscii(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) out.push_back(static_cast<char>(std::tolower(c)));
  return out;
}

bool ParseBool(std::string_view s, bool* out) {
  std::string v = LowerAscii(s);
  if (v == "1" || v == "true" || v == "yes" || v == "on") {
    *out = true;
    return true;
  }
  if (v == "0" || v == "false" || v == "no" || v == "off") {
    *out = false;
    return true;
  }
  return false;
}

bool ParseUint64(std::string_view s, std::uint64_t* out) {
  if (s.empty()) return false;
  std::uint64_t v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size()) return false;
  *out = v;
  return true;
}

bool ParseInt(std::string_view s, int* out) {
  if (s.empty()) return false;
  int v = 0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || ptr != s.data() + s.size()) return false;
  *out = v;
  return true;
}

bool ParseByteSize(std::string_view s, std::uint64_t* out) {
  if (s.empty()) return false;
  std::size_t i = 0;
  while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
  if (i == 0) return false;
  std::string_view num_sv = s.substr(0, i);
  std::string suffix = LowerAscii(s.substr(i));
  std::uint64_t base = 0;
  if (!ParseUint64(num_sv, &base)) return false;

  std::uint64_t mul = 1;
  if (suffix.empty() || suffix == "b") {
    mul = 1;
  } else if (suffix == "k" || suffix == "kb" || suffix == "kib") {
    mul = 1024ULL;
  } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
    mul = 1024ULL * 1024ULL;
  } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
    mul = 1024ULL * 1024ULL * 1024ULL;
  } else {
    return false;
  }
  if (base > (std::numeric_limits<std::uint64_t>::max() / mul)) return false;
  *out = base * mul;
  return true;
}

void PrintUsage(const char* prog) {
  std::cerr << "Usage:\n";
  std::cerr << "  " << prog << " [server options] [dkv options]\n\n";
  std::cerr << "Server options:\n";
  std::cerr << "  --bind <ip>                  (default: 0.0.0.0)\n";
  std::cerr << "  --port <port>                (default: 6379)\n";
  std::cerr << "  --subreactors <n>             (0 = auto)\n";
  std::cerr << "  --workers <n>                 (0 = auto)\n\n";
  std::cerr << "DKV options (mirrors include/dkv/options.h):\n";
  std::cerr << "  --data-dir <path>                         (default: dkv-data)\n";
  std::cerr << "  --memtable-soft-limit-bytes <bytes>       (*-bytes supports K/M/G suffix)\n";
  std::cerr << "  --sync-wal [true|false]                   (flag means true)\n";
  std::cerr << "  --sstable-target-size-bytes <bytes>\n";
  std::cerr << "  --sstable-block-size-bytes <bytes>\n";
  std::cerr << "  --bloom-bits-per-key <n>\n";
  std::cerr << "  --bloom-cache-capacity-bytes <bytes>\n";
  std::cerr << "  --raw-block-cache-capacity-bytes <bytes>\n";
  std::cerr << "  --block-cache-capacity-bytes <bytes>\n";
  std::cerr << "  --level0-file-limit <n>\n";
  std::cerr << "  --level-base-bytes <bytes>\n";
  std::cerr << "  --level-size-multiplier <n>\n";
  std::cerr << "  --max-levels <n>\n";
  std::cerr << "  --flush-thread-count <n>\n";
  std::cerr << "  --max-immutable-memtables <n>\n";
  std::cerr << "  --compaction-thread-count <n>\n";
  std::cerr << "  --wal-sync-interval-ms <ms>\n";
  std::cerr << "  --enable-crc [true|false]\n";
  std::cerr << "  --verify-sstable-crc [true|false]\n";
  std::cerr << "  --enable-compress [true|false]\n";
}

enum class ParseOutcome { kOk, kHelp, kError };

ParseOutcome ParseArgs(int argc, char** argv, dkv_server::ServerConfig* cfg, std::string* err) {
  auto fail = [&](std::string msg) {
    *err = std::move(msg);
    return ParseOutcome::kError;
  };

  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg == "--help" || arg == "-h") return ParseOutcome::kHelp;
    if (arg == "--") break;
    if (!arg.starts_with("--")) return fail("unexpected argument: " + std::string(arg));

    std::string_view key = arg;
    std::optional<std::string_view> inline_value;
    if (auto eq = arg.find('='); eq != std::string_view::npos) {
      key = arg.substr(0, eq);
      inline_value = arg.substr(eq + 1);
    }

    auto require_value = [&]() -> std::optional<std::string_view> {
      if (inline_value) return inline_value;
      if (i + 1 >= argc) return std::nullopt;
      std::string_view next(argv[i + 1]);
      if (next.starts_with("--")) return std::nullopt;
      ++i;
      return next;
    };

    auto optional_bool_value = [&]() -> std::optional<bool> {
      if (inline_value) {
        bool b = false;
        if (!ParseBool(*inline_value, &b)) return std::nullopt;
        return b;
      }
      if (i + 1 < argc) {
        std::string_view next(argv[i + 1]);
        bool b = false;
        if (!next.starts_with("--") && ParseBool(next, &b)) {
          ++i;
          return b;
        }
      }
      return std::nullopt;
    };

    if (key == "--bind") {
      auto v = require_value();
      if (!v) return fail("--bind requires a value");
      cfg->bind = std::string(*v);
      continue;
    }
    if (key == "--port") {
      auto v = require_value();
      if (!v) return fail("--port requires a value");
      int port = 0;
      if (!ParseInt(*v, &port) || port <= 0 || port > 65535) return fail("invalid --port: " + std::string(*v));
      cfg->port = port;
      continue;
    }
    if (key == "--subreactors") {
      auto v = require_value();
      if (!v) return fail("--subreactors requires a value");
      std::uint64_t n = 0;
      if (!ParseUint64(*v, &n) || n > std::numeric_limits<std::size_t>::max()) {
        return fail("invalid --subreactors: " + std::string(*v));
      }
      cfg->subreactors = static_cast<std::size_t>(n);
      continue;
    }
    if (key == "--workers") {
      auto v = require_value();
      if (!v) return fail("--workers requires a value");
      std::uint64_t n = 0;
      if (!ParseUint64(*v, &n) || n > std::numeric_limits<std::size_t>::max()) {
        return fail("invalid --workers: " + std::string(*v));
      }
      cfg->workers = static_cast<std::size_t>(n);
      continue;
    }

    auto set_bytes = [&](std::size_t* field, std::string_view opt_name) -> ParseOutcome {
      auto v = require_value();
      if (!v) return fail(std::string(opt_name) + " requires a value");
      std::uint64_t bytes = 0;
      if (!ParseByteSize(*v, &bytes) || bytes > std::numeric_limits<std::size_t>::max()) {
        return fail(std::string("invalid ") + std::string(opt_name) + ": " + std::string(*v));
      }
      *field = static_cast<std::size_t>(bytes);
      return ParseOutcome::kOk;
    };

    auto set_u64 = [&](std::size_t* field, std::string_view opt_name) -> ParseOutcome {
      auto v = require_value();
      if (!v) return fail(std::string(opt_name) + " requires a value");
      std::uint64_t n = 0;
      if (!ParseUint64(*v, &n) || n > std::numeric_limits<std::size_t>::max()) {
        return fail(std::string("invalid ") + std::string(opt_name) + ": " + std::string(*v));
      }
      *field = static_cast<std::size_t>(n);
      return ParseOutcome::kOk;
    };

    auto set_bool = [&](bool* field) -> ParseOutcome {
      auto b = optional_bool_value();
      if (b) {
        *field = *b;
      } else {
        *field = true;
      }
      return ParseOutcome::kOk;
    };

    if (key == "--data-dir") {
      auto v = require_value();
      if (!v) return fail("--data-dir requires a value");
      cfg->dkv_options.data_dir = std::string(*v);
      continue;
    }
    if (key == "--memtable-soft-limit-bytes") {
      auto r = set_bytes(&cfg->dkv_options.memtable_soft_limit_bytes, "--memtable-soft-limit-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--sync-wal") {
      auto r = set_bool(&cfg->dkv_options.sync_wal);
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--sstable-target-size-bytes") {
      auto r = set_bytes(&cfg->dkv_options.sstable_target_size_bytes, "--sstable-target-size-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--sstable-block-size-bytes") {
      auto r = set_bytes(&cfg->dkv_options.sstable_block_size_bytes, "--sstable-block-size-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--bloom-bits-per-key") {
      auto r = set_u64(&cfg->dkv_options.bloom_bits_per_key, "--bloom-bits-per-key");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--bloom-cache-capacity-bytes") {
      auto r = set_bytes(&cfg->dkv_options.bloom_cache_capacity_bytes, "--bloom-cache-capacity-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--raw-block-cache-capacity-bytes") {
      auto r = set_bytes(&cfg->dkv_options.raw_block_cache_capacity_bytes, "--raw-block-cache-capacity-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--block-cache-capacity-bytes") {
      auto r = set_bytes(&cfg->dkv_options.block_cache_capacity_bytes, "--block-cache-capacity-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--level0-file-limit") {
      auto r = set_u64(&cfg->dkv_options.level0_file_limit, "--level0-file-limit");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--level-base-bytes") {
      auto r = set_bytes(&cfg->dkv_options.level_base_bytes, "--level-base-bytes");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--level-size-multiplier") {
      auto r = set_u64(&cfg->dkv_options.level_size_multiplier, "--level-size-multiplier");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--max-levels") {
      auto r = set_u64(&cfg->dkv_options.max_levels, "--max-levels");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--flush-thread-count") {
      auto r = set_u64(&cfg->dkv_options.flush_thread_count, "--flush-thread-count");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--max-immutable-memtables") {
      auto r = set_u64(&cfg->dkv_options.max_immutable_memtables, "--max-immutable-memtables");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--compaction-thread-count") {
      auto r = set_u64(&cfg->dkv_options.compaction_thread_count, "--compaction-thread-count");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--wal-sync-interval-ms") {
      auto r = set_u64(&cfg->dkv_options.wal_sync_interval_ms, "--wal-sync-interval-ms");
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--enable-crc") {
      auto r = set_bool(&cfg->dkv_options.enable_crc);
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--verify-sstable-crc") {
      auto r = set_bool(&cfg->dkv_options.verify_sstable_crc);
      if (r != ParseOutcome::kOk) return r;
      continue;
    }
    if (key == "--enable-compress") {
      auto r = set_bool(&cfg->dkv_options.enable_compress);
      if (r != ParseOutcome::kOk) return r;
      continue;
    }

    return fail("unknown option: " + std::string(key));
  }

  return ParseOutcome::kOk;
}
}  // namespace

int main(int argc, char** argv) {
  dkv_server::ServerConfig cfg;
  std::string err;
  switch (ParseArgs(argc, argv, &cfg, &err)) {
    case ParseOutcome::kOk:
      break;
    case ParseOutcome::kHelp:
      PrintUsage(argv[0]);
      return 0;
    case ParseOutcome::kError:
      std::cerr << "error: " << err << "\n\n";
      PrintUsage(argv[0]);
      return 1;
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  std::signal(SIGSEGV, OnFatalSignal);
  std::signal(SIGABRT, OnFatalSignal);
#ifdef SIGBUS
  std::signal(SIGBUS, OnFatalSignal);
#endif
#ifdef SIGILL
  std::signal(SIGILL, OnFatalSignal);
#endif

  try {
    dkv_server::DkvServer server(cfg);
    server.Start();
    while (!g_stop.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    server.Stop();
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "fatal: " << ex.what() << "\n";
    return 1;
  }
}
