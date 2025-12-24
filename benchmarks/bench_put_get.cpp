#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "dkv/db.h"

#ifndef DKV_HAVE_SQLITE
#define DKV_HAVE_SQLITE 0
#endif

#if DKV_HAVE_SQLITE
#include <sqlite3.h>
#endif

namespace {

struct CrudStats {
  double put_ms{0};
  double get_ms{0};
  double update_ms{0};
  double delete_ms{0};
};

double OpsPerSec(std::size_t count, double ms) {
  if (ms <= 0.0) return 0.0;
  return static_cast<double>(count) / (ms / 1000.0);
}

std::vector<std::string> Keys(std::size_t n) {
  std::vector<std::string> keys;
  keys.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    keys.push_back("key" + std::to_string(i));
  }
  return keys;
}

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

CrudStats BenchDKVSingle(std::size_t n) {
  CrudStats stats;
  auto dir = TempDir("dkv-bench-single");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 64 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;
  opts.wal_sync_interval_ms = 5;
  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv single");

  auto keys = Keys(n);
  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  auto start_put = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Put(wopts, keys[i], "v" + std::to_string(i)), "dkv put");
  }
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Get(ropts, keys[i], value), "dkv get");
  }
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Put(wopts, keys[i], "u" + std::to_string(i)), "dkv update");
  }
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Delete(wopts, keys[i]), "dkv delete");
  }
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

CrudStats BenchDKVBatch(std::size_t n, std::size_t batch_size) {
  CrudStats stats;
  auto dir = TempDir("dkv-bench-batch");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 64 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;
  opts.wal_sync_interval_ms = 5;

  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv batch");

  auto keys = Keys(n);
  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  auto start_put = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    dkv::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Put(keys[j], "v" + std::to_string(j));
    }
    EnsureOk(db->Write(wopts, batch), "dkv batch put");
  }
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    EnsureOk(db->Get(ropts, keys[i], value), "dkv batch get");
  }
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    dkv::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Put(keys[j], "u" + std::to_string(j));
    }
    EnsureOk(db->Write(wopts, batch), "dkv batch update");
  }
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    dkv::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Delete(keys[j]);
    }
    EnsureOk(db->Write(wopts, batch), "dkv batch delete");
  }
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

#if DKV_HAVE_SQLITE
bool CheckSQLite(int rc, sqlite3* db, const char* msg) {
  if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
    std::cerr << msg << ": " << sqlite3_errmsg(db) << "\n";
    return false;
  }
  return true;
}

void RequireSQLite(int rc, sqlite3* db, const char* msg) {
  if (!CheckSQLite(rc, db, msg)) std::exit(1);
}

CrudStats BenchSQLite(std::size_t n, std::size_t batch_size) {
  (void)batch_size;
  CrudStats stats;
  auto dir = TempDir("sqlite-bench");
  auto db_path = dir / "bench.db";

  sqlite3* db = nullptr;
  RequireSQLite(sqlite3_open(db_path.string().c_str(), &db), db, "open sqlite");

  RequireSQLite(sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr), db,
                "pragma wal");
  RequireSQLite(sqlite3_exec(db, "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr), db,
                "pragma sync");
  RequireSQLite(sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);",
                             nullptr, nullptr, nullptr),
                db, "create table");

  sqlite3_stmt* put_stmt = nullptr;
  sqlite3_stmt* get_stmt = nullptr;
  sqlite3_stmt* del_stmt = nullptr;
  RequireSQLite(sqlite3_prepare_v2(db, "REPLACE INTO kv(k,v) VALUES(?,?);", -1, &put_stmt, nullptr),
                db, "prepare put");
  RequireSQLite(sqlite3_prepare_v2(db, "SELECT v FROM kv WHERE k=?;", -1, &get_stmt, nullptr), db,
                "prepare get");
  RequireSQLite(sqlite3_prepare_v2(db, "DELETE FROM kv WHERE k=?;", -1, &del_stmt, nullptr), db,
                "prepare delete");

  auto keys = Keys(n);

  auto start_put = std::chrono::steady_clock::now();
  RequireSQLite(sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr), db, "begin put");
  for (std::size_t i = 0; i < n; ++i) {
    sqlite3_bind_text(put_stmt, 1, keys[i].c_str(), -1, SQLITE_STATIC);
    auto value = "v" + std::to_string(i);
    sqlite3_bind_text(put_stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
    RequireSQLite(sqlite3_step(put_stmt), db, "put step");
    sqlite3_reset(put_stmt);
  }
  RequireSQLite(sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr), db, "commit put");
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  auto start_get = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    sqlite3_bind_text(get_stmt, 1, keys[i].c_str(), -1, SQLITE_STATIC);
    RequireSQLite(sqlite3_step(get_stmt), db, "get step");
    sqlite3_reset(get_stmt);
  }
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  RequireSQLite(sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr), db, "begin update");
  for (std::size_t i = 0; i < n; ++i) {
    sqlite3_bind_text(put_stmt, 1, keys[i].c_str(), -1, SQLITE_STATIC);
    auto value = "u" + std::to_string(i);
    sqlite3_bind_text(put_stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
    RequireSQLite(sqlite3_step(put_stmt), db, "update step");
    sqlite3_reset(put_stmt);
  }
  RequireSQLite(sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr), db, "commit update");
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  RequireSQLite(sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr), db, "begin delete");
  for (std::size_t i = 0; i < n; ++i) {
    sqlite3_bind_text(del_stmt, 1, keys[i].c_str(), -1, SQLITE_STATIC);
    RequireSQLite(sqlite3_step(del_stmt), db, "delete step");
    sqlite3_reset(del_stmt);
  }
  RequireSQLite(sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr), db, "commit delete");
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  sqlite3_finalize(put_stmt);
  sqlite3_finalize(get_stmt);
  sqlite3_finalize(del_stmt);
  sqlite3_close(db);

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}
#endif  // DKV_HAVE_SQLITE

void PrintStats(const std::string& label, std::size_t n, const CrudStats& s) {
  std::cout << label << " put:    " << s.put_ms << " ms  (" << OpsPerSec(n, s.put_ms)
            << " ops/sec)\n";
  std::cout << label << " get:    " << s.get_ms << " ms  (" << OpsPerSec(n, s.get_ms)
            << " ops/sec)\n";
  std::cout << label << " update: " << s.update_ms << " ms  (" << OpsPerSec(n, s.update_ms)
            << " ops/sec)\n";
  std::cout << label << " delete: " << s.delete_ms << " ms  (" << OpsPerSec(n, s.delete_ms)
            << " ops/sec)\n";
}

}  // namespace

int main() {
  const std::size_t kN = 100000;
  const std::size_t kBatch = 5000;

  std::cout << "Benchmarking " << kN << " ops\n";

  auto dkv_single = BenchDKVSingle(kN);
  std::cout << "\nDKV (single ops)\n";
  PrintStats("  ", kN, dkv_single);

  auto dkv_batch = BenchDKVBatch(kN, kBatch);
  std::cout << "\nDKV (batched writes, batch size " << kBatch << ")\n";
  PrintStats("  ", kN, dkv_batch);

#if DKV_HAVE_SQLITE
  auto sqlite_stats = BenchSQLite(kN, kBatch);
  std::cout << "\nSQLite (txn + prepared statements)\n";
  PrintStats("  ", kN, sqlite_stats);
#else
  std::cout << "\nSQLite bench skipped (DKV_HAVE_SQLITE=0)\n";
#endif

  return 0;
}
