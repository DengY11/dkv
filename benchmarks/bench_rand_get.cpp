#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "dkv/db.h"

namespace {

struct Args {
  std::filesystem::path data_dir;
  std::uint64_t n{10'000'000};
  std::uint64_t key_space{1'000'000};
  std::size_t value_size{100};
  std::size_t threads{8};
  std::uint64_t seed{12345};
  bool prepare{false};
  bool compact{false};
  std::size_t block_cache_mb{0};
  std::size_t bloom_cache_mb{0};
  std::size_t raw_cache_mb{0};
};

bool ParseBool(std::string_view v, bool* out) {
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

bool ParseArgs(int argc, char** argv, Args& args) {
  for (int i = 1; i < argc; ++i) {
    std::string_view a(argv[i]);
    auto take = [&](std::string_view key) -> std::string_view {
      if (a == key) {
        if (i + 1 >= argc) return {};
        return std::string_view(argv[++i]);
      }
      if (a.rfind(key, 0) == 0 && a.size() > key.size() && a[key.size()] == '=') {
        return a.substr(key.size() + 1);
      }
      return {};
    };

    if (auto v = take("--dir"); !v.empty()) {
      args.data_dir = std::filesystem::path(std::string(v));
      continue;
    }
    if (auto v = take("--n"); !v.empty()) {
      args.n = static_cast<std::uint64_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--keys"); !v.empty()) {
      args.key_space = static_cast<std::uint64_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--value_size"); !v.empty()) {
      args.value_size = static_cast<std::size_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--threads"); !v.empty()) {
      args.threads = static_cast<std::size_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--seed"); !v.empty()) {
      args.seed = static_cast<std::uint64_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--prepare"); !v.empty()) {
      if (!ParseBool(v, &args.prepare)) return false;
      continue;
    }
    if (auto v = take("--compact"); !v.empty()) {
      if (!ParseBool(v, &args.compact)) return false;
      continue;
    }
    if (auto v = take("--block_cache_mb"); !v.empty()) {
      args.block_cache_mb = static_cast<std::size_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--bloom_cache_mb"); !v.empty()) {
      args.bloom_cache_mb = static_cast<std::size_t>(std::stoull(std::string(v)));
      continue;
    }
    if (auto v = take("--raw_cache_mb"); !v.empty()) {
      args.raw_cache_mb = static_cast<std::size_t>(std::stoull(std::string(v)));
      continue;
    }
    if (a == "--help" || a == "-h") return false;
    std::cerr << "unknown arg: " << a << "\n";
    return false;
  }
  if (args.threads == 0) args.threads = 1;
  if (args.key_space == 0) args.key_space = 1;
  return true;
}

int FormatKeyTo(char* buf, std::size_t cap, std::uint64_t i) {
  return std::snprintf(buf, cap, "k%016llu", static_cast<unsigned long long>(i));
}

void PrepareDB(const Args& args) {
  std::error_code ec;
  std::filesystem::remove_all(args.data_dir, ec);
  std::filesystem::create_directories(args.data_dir, ec);

  dkv::Options opts;
  opts.data_dir = args.data_dir;
  opts.memtable_soft_limit_bytes = 64 * 1024 * 1024;
  opts.flush_thread_count = 1;
  opts.compaction_thread_count = 1;

  std::unique_ptr<dkv::DB> db;
  auto s = dkv::DB::Open(opts, db);
  if (!s.ok()) {
    std::cerr << "open prepare failed: " << s.ToString() << "\n";
    std::exit(2);
  }

  dkv::WriteBatch batch;
  const std::string value(args.value_size, 'v');
  for (std::uint64_t i = 0; i < args.key_space; ++i) {
    char kbuf[32];
    int klen = FormatKeyTo(kbuf, sizeof(kbuf), i);
    batch.Put(std::string(kbuf, static_cast<std::size_t>(klen)), value);
    if (batch.ops().size() >= 1024) {
      auto ws = db->Write(dkv::WriteOptions{}, batch);
      if (!ws.ok()) {
        std::cerr << "prepare write failed: " << ws.ToString() << "\n";
        std::exit(2);
      }
      batch.Clear();
    }
  }
  if (!batch.empty()) {
    auto ws = db->Write(dkv::WriteOptions{}, batch);
    if (!ws.ok()) {
      std::cerr << "prepare write failed: " << ws.ToString() << "\n";
      std::exit(2);
    }
  }

  auto fs = db->Flush();
  if (!fs.ok()) {
    std::cerr << "prepare flush failed: " << fs.ToString() << "\n";
    std::exit(2);
  }
  if (args.compact) {
    auto cs = db->Compact();
    if (!cs.ok()) {
      std::cerr << "prepare compact failed: " << cs.ToString() << "\n";
      std::exit(2);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!ParseArgs(argc, argv, args)) {
    std::cerr << "Usage: dkv_bench_rand_get [--dir PATH] [--prepare 0/1] [--compact 0/1] "
                 "[--n N] [--keys K] [--value_size BYTES] [--threads T] [--seed S] "
                 "[--block_cache_mb MB] [--bloom_cache_mb MB] [--raw_cache_mb MB]\n";
    return 2;
  }

  if (args.data_dir.empty()) {
    args.data_dir = std::filesystem::temp_directory_path() / "dkv-rand-get";
  }

  if (args.prepare) PrepareDB(args);

  dkv::Options opts;
  opts.data_dir = args.data_dir;
  opts.flush_thread_count = 1;
  opts.compaction_thread_count = 1;
  opts.block_cache_capacity_bytes = args.block_cache_mb * 1024ULL * 1024ULL;
  opts.bloom_cache_capacity_bytes = args.bloom_cache_mb * 1024ULL * 1024ULL;
  opts.raw_block_cache_capacity_bytes = args.raw_cache_mb * 1024ULL * 1024ULL;

  std::unique_ptr<dkv::DB> db;
  auto s = dkv::DB::Open(opts, db);
  if (!s.ok()) {
    std::cerr << "open failed: " << s.ToString() << "\n";
    return 2;
  }

  std::cout << "dkv rand-get bench\n"
            << "  dir=" << args.data_dir << "\n"
            << "  n=" << args.n << " keys=" << args.key_space << " value_size=" << args.value_size
            << " threads=" << args.threads << " seed=" << args.seed << "\n"
            << "  caches: block=" << args.block_cache_mb << "MB bloom=" << args.bloom_cache_mb
            << "MB raw=" << args.raw_cache_mb << "MB\n";

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::atomic<std::uint64_t> hits{0}, misses{0}, errors{0};

  auto worker = [&](std::size_t tid) {
    std::mt19937_64 rng(args.seed + tid * 0x9e3779b97f4a7c15ULL);
    std::uniform_int_distribution<std::uint64_t> dist(0, args.key_space - 1);
    dkv::ReadOptions ro;
    ro.fill_cache = true;
    std::string value;
    char key_buf[32];

    ready.fetch_add(1, std::memory_order_release);
    while (!go.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    const std::uint64_t ops = args.n / args.threads + (tid < (args.n % args.threads) ? 1 : 0);
    for (std::uint64_t i = 0; i < ops; ++i) {
      const auto k = dist(rng);
      int klen = FormatKeyTo(key_buf, sizeof(key_buf), k);
      auto gs = db->Get(ro, std::string_view(key_buf, static_cast<std::size_t>(klen)), value);
      if (gs.ok()) {
        hits.fetch_add(1, std::memory_order_relaxed);
      } else if (gs.code() == dkv::Status::Code::kNotFound) {
        misses.fetch_add(1, std::memory_order_relaxed);
      } else {
        errors.fetch_add(1, std::memory_order_relaxed);
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(args.threads);
  for (std::size_t t = 0; t < args.threads; ++t) threads.emplace_back(worker, t);
  while (ready.load(std::memory_order_acquire) != static_cast<int>(args.threads)) {
    std::this_thread::yield();
  }
  auto start = std::chrono::steady_clock::now();
  go.store(true, std::memory_order_release);
  for (auto& th : threads) th.join();
  auto end = std::chrono::steady_clock::now();

  const double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
  const std::uint64_t total = hits.load() + misses.load() + errors.load();
  std::cout << "  time=" << seconds << " sec\n"
            << "  ops=" << total << " hits=" << hits.load() << " misses=" << misses.load()
            << " errors=" << errors.load() << "\n"
            << "  throughput=" << (seconds > 0 ? static_cast<double>(total) / seconds : 0.0) << " ops/sec\n";
  return errors.load() ? 1 : 0;
}
