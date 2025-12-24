#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "dkv/db.h"

namespace {

std::filesystem::path TempDir(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  std::filesystem::create_directories(dir, ec);
  return dir;
}

void EnsureOk(const dkv::Status& s, const std::string& msg) {
  if (!s.ok()) {
    std::cerr << msg << ": " << s.ToString() << "\n";
    std::exit(1);
  }
}

std::vector<std::string> MakeStrings(std::size_t n, std::size_t len, char prefix) {
  std::vector<std::string> out;
  out.reserve(n);
  std::string base(len, prefix);
  for (std::size_t i = 0; i < n; ++i) {
    auto s = base;
    if (len >= sizeof(std::size_t)) {
      std::memcpy(&s[len - sizeof(std::size_t)], &i, sizeof(std::size_t));
    }
    out.push_back(std::move(s));
  }
  return out;
}

void BenchPut(std::size_t n, std::size_t key_len, std::size_t value_len) {
  auto dir = TempDir("dkv-put-only");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 512 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;

  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv");

  auto keys = MakeStrings(n, key_len, 'k');
  auto values = MakeStrings(n, value_len, 'v');
  dkv::WriteOptions wopts;

  auto start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Put(wopts, keys[i], values[i]), "put");
  }
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
          .count();

  double ops_sec = (elapsed_ms > 0) ? static_cast<double>(n) / (elapsed_ms / 1000.0) : 0.0;
  std::cout << "Put " << n << " entries "
            << "(key_len=" << key_len << ", value_len=" << value_len << "): "
            << elapsed_ms << " ms, " << ops_sec << " ops/sec\n";

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

}  // namespace

int main() {
  const std::size_t n = 500000;
  const std::size_t key_len = 16;
  const std::size_t value_len = 100;
  BenchPut(n, key_len, value_len);
  return 0;
}
