#include <algorithm>
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

void FillStrings(std::vector<std::string>& out, std::size_t start, std::size_t count, std::size_t len,
                 char prefix) {
  out.resize(count);
  std::string base(len, prefix);
  for (std::size_t i = 0; i < count; ++i) {
    auto s = base;
    if (len >= sizeof(std::size_t)) {
      const auto id = start + i;
      std::memcpy(&s[len - sizeof(std::size_t)], &id, sizeof(std::size_t));
    }
    out[i].swap(s);
  }
}

void BenchPut(std::size_t n, std::size_t key_len, std::size_t value_len, std::size_t batch_size) {
  auto dir = TempDir("dkv-put-only");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 512 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 12;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;

  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv");

  std::vector<std::string> keys;
  std::vector<std::string> values;
  dkv::WriteOptions wopts;

  auto start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    const auto count = std::min(batch_size, n - i);
    FillStrings(keys, i, count, key_len, 'k');
    FillStrings(values, i, count, value_len, 'v');
    for (std::size_t j = 0; j < count; ++j) {
      EnsureOk(db->Put(wopts, keys[j], values[j]), "put");
    }
  }
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
          .count();

  double secs = (elapsed_ms > 0) ? (elapsed_ms / 1000.0) : 0.0;
  double ops_sec = (secs > 0) ? static_cast<double>(n) / secs : 0.0;
  // Payload MB/s: only key+value bytes (no headers), base 1 MB = 1e6 bytes.
  double payload_bytes = static_cast<double>(n) * (key_len + value_len);
  double payload_mb_s = (secs > 0) ? (payload_bytes / 1e6) / secs : 0.0;
  std::cout << "Put " << n << " entries "
            << "(key_len=" << key_len << ", value_len=" << value_len << "): "
            << elapsed_ms << " ms, " << ops_sec << " ops/sec, "
            << payload_mb_s << " MB/s payload\n";

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

}  // namespace

int main() {
  const std::size_t n = 5000000;
  const std::size_t key_len = 16;
  const std::size_t value_len = 16;
  const std::size_t batch_size = 50000;
  BenchPut(n, key_len, value_len, batch_size);
  return 0;
}
