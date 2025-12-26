#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "dkv/db.h"

#ifndef DKV_HAVE_SQLITE
#define DKV_HAVE_SQLITE 0
#endif
#ifndef DKV_HAVE_LEVELDB
#define DKV_HAVE_LEVELDB 0
#endif

#if DKV_HAVE_SQLITE
#include <sqlite3.h>
#endif
#if DKV_HAVE_LEVELDB
#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
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
  opts.memtable_soft_limit_bytes = 1024 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;
  opts.wal_sync_interval_ms = 5;
  opts.memtable_shard_count = 4;
  opts.bloom_cache_capacity_bytes = 512 * 1024 * 1024;
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

  db.reset();  // ensure background flush/threads stop before deleting directory
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

CrudStats BenchDKVBatch(std::size_t n, std::size_t batch_size) {
  CrudStats stats;
  auto dir = TempDir("dkv-bench-batch");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 1024 * 1024 * 1024;
  opts.sync_wal = false;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;
  opts.wal_sync_interval_ms = 5;
  opts.bloom_cache_capacity_bytes = 512 * 1024 * 1024;

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

  db.reset();  // ensure background flush/threads stop before deleting directory
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

CrudStats BenchDKVMultithread(std::size_t n_threads, std::size_t ops_per_thread) {
  CrudStats stats;
  auto dir = TempDir("dkv-bench-mt");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 512 * 1024 * 1024;
  opts.sync_wal = false;
  opts.wal_sync_interval_ms = 0;  // focus on pure concurrency, no background sync

  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv mt");

  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  // Generate unique keys per thread to avoid overwriting each other's data.
  auto worker = [&](std::size_t tid, std::size_t count, double& put_ms, double& get_ms) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      keys.push_back("t" + std::to_string(tid) + "-k" + std::to_string(i));
    }
    auto start_put = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
      EnsureOk(db->Put(wopts, keys[i], "v" + std::to_string(i)), "mt put");
    }
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
      EnsureOk(db->Get(ropts, keys[i], value), "mt get");
    }
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(n_threads, 0.0);
  std::vector<double> get_times(n_threads, 0.0);

  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < n_threads; ++t) {
    threads.emplace_back(worker, t, ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all)
          .count();

  // Aggregate: total ops = threads * ops_per_thread.
  const std::size_t total_ops = n_threads * ops_per_thread;
  // For per-op latency, use slowest thread; for throughput, use wall-clock.
  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < n_threads; ++i) {
    worst_put_ms = std::max(worst_put_ms, put_times[i]);
    worst_get_ms = std::max(worst_get_ms, get_times[i]);
  }
  stats.put_ms = worst_put_ms;
  stats.get_ms = worst_get_ms;
  // Store update/delete as total wall clock to keep PrintStats format.
  stats.update_ms = elapsed_total;
  stats.delete_ms = 0;

  db.reset();  // ensure background threads stop before deleting directory
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

#if DKV_HAVE_LEVELDB
CrudStats BenchLevelDB(std::size_t n, std::size_t batch_size) {
  CrudStats stats;
  auto dir = TempDir("leveldb-bench");

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.compression = leveldb::kNoCompression;
  opts.block_size = 16 * 1024;
  opts.write_buffer_size = 1024 * 1024 * 1024;  // match DKV memtable limit used here
  opts.max_open_files = 1000;
  std::unique_ptr<leveldb::DB> db;
  {
    leveldb::DB* raw = nullptr;
    auto s = leveldb::DB::Open(opts, dir.string(), &raw);
    if (!s.ok()) {
      std::cerr << "open leveldb: " << s.ToString() << "\n";
      std::exit(1);
    }
    db.reset(raw);
  }

  auto keys = Keys(n);
  dkv::WriteOptions wopts;

  leveldb::WriteOptions lw;
  lw.sync = false;

  auto start_put = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    leveldb::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Put(keys[j], "v" + std::to_string(j));
    }
    auto s = db->Write(lw, &batch);
    if (!s.ok()) {
      std::cerr << "leveldb batch put: " << s.ToString() << "\n";
      std::exit(1);
    }
  }
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; ++i) {
    auto s = db->Get(leveldb::ReadOptions(), keys[i], &value);
    if (!s.ok()) {
      std::cerr << "leveldb get: " << s.ToString() << "\n";
      std::exit(1);
    }
  }
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    leveldb::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Put(keys[j], "u" + std::to_string(j));
    }
    auto s = db->Write(lw, &batch);
    if (!s.ok()) {
      std::cerr << "leveldb batch update: " << s.ToString() << "\n";
      std::exit(1);
    }
  }
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < n; i += batch_size) {
    leveldb::WriteBatch batch;
    for (std::size_t j = i; j < std::min(n, i + batch_size); ++j) {
      batch.Delete(keys[j]);
    }
    auto s = db->Write(lw, &batch);
    if (!s.ok()) {
      std::cerr << "leveldb batch delete: " << s.ToString() << "\n";
      std::exit(1);
    }
  }
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  db.reset();
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

CrudStats BenchLevelDBMultithread(std::size_t n_threads, std::size_t ops_per_thread) {
  CrudStats stats;
  auto dir = TempDir("leveldb-bench-mt");

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.compression = leveldb::kNoCompression;
  opts.block_size = 16 * 1024;
  opts.write_buffer_size = 1024 * 1024 * 1024;
  opts.max_open_files = 1000;
  std::unique_ptr<leveldb::DB> db;
  {
    leveldb::DB* raw = nullptr;
    auto s = leveldb::DB::Open(opts, dir.string(), &raw);
    if (!s.ok()) {
      std::cerr << "open leveldb mt: " << s.ToString() << "\n";
      std::exit(1);
    }
    db.reset(raw);
  }

  auto worker = [&](std::size_t tid, std::size_t count, double& put_ms, double& get_ms) {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      keys.push_back("t" + std::to_string(tid) + "-k" + std::to_string(i));
    }
    leveldb::WriteOptions lw;
    lw.sync = false;

    auto start_put = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
      auto s = db->Put(lw, keys[i], "v" + std::to_string(i));
      if (!s.ok()) {
        std::cerr << "leveldb mt put: " << s.ToString() << "\n";
        std::exit(1);
      }
    }
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
      auto s = db->Get(leveldb::ReadOptions(), keys[i], &value);
      if (!s.ok()) {
        std::cerr << "leveldb mt get: " << s.ToString() << "\n";
        std::exit(1);
      }
    }
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(n_threads, 0.0);
  std::vector<double> get_times(n_threads, 0.0);

  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < n_threads; ++t) {
    threads.emplace_back(worker, t, ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all)
          .count();

  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < n_threads; ++i) {
    worst_put_ms = std::max(worst_put_ms, put_times[i]);
    worst_get_ms = std::max(worst_get_ms, get_times[i]);
  }
  stats.put_ms = worst_put_ms;
  stats.get_ms = worst_get_ms;
  stats.update_ms = elapsed_total;
  stats.delete_ms = 0;

  db.reset();
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}
#endif  // DKV_HAVE_LEVELDB

#if DKV_HAVE_SQLITE
// Forward declarations for helpers defined below.
bool CheckSQLite(int rc, sqlite3* db, const char* msg);
void RequireSQLite(int rc, sqlite3* db, const char* msg);
CrudStats BenchSQLiteMultithread(std::size_t n_threads, std::size_t ops_per_thread) {
  CrudStats stats;
  auto dir = TempDir("sqlite-bench-mt");
  auto db_path = dir / "bench.db";

  // Set up DB and WAL mode once to avoid pragma lock contention.
  {
    sqlite3* db = nullptr;
    RequireSQLite(sqlite3_open(db_path.string().c_str(), &db), db, "mt init open");
    RequireSQLite(sqlite3_busy_timeout(db, 10000), db, "mt init busy timeout");
    RequireSQLite(sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr), db,
                  "mt init pragma wal");
    RequireSQLite(sqlite3_exec(db, "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr), db,
                  "mt init pragma sync");
    RequireSQLite(sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);",
                               nullptr, nullptr, nullptr),
                  db, "mt init create");
    sqlite3_close(db);
  }

  auto worker = [&](std::size_t tid, std::size_t count, double& put_ms, double& get_ms) {
    sqlite3* db = nullptr;
    RequireSQLite(sqlite3_open(db_path.string().c_str(), &db), db, "mt open sqlite");
    RequireSQLite(sqlite3_busy_timeout(db, 10000), db, "mt busy timeout");

    sqlite3_stmt* put_stmt = nullptr;
    sqlite3_stmt* get_stmt = nullptr;
    RequireSQLite(sqlite3_prepare_v2(db, "REPLACE INTO kv(k,v) VALUES(?,?);", -1, &put_stmt, nullptr),
                  db, "mt prepare put");
    RequireSQLite(sqlite3_prepare_v2(db, "SELECT v FROM kv WHERE k=?;", -1, &get_stmt, nullptr), db,
                  "mt prepare get");

    auto start_put = std::chrono::steady_clock::now();
    RequireSQLite(sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr), db, "mt begin put");
    for (std::size_t i = 0; i < count; ++i) {
      auto key = "t" + std::to_string(tid) + "-k" + std::to_string(i);
      auto value = "v" + std::to_string(i);
      sqlite3_bind_text(put_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(put_stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
      RequireSQLite(sqlite3_step(put_stmt), db, "mt put step");
      sqlite3_reset(put_stmt);
    }
    RequireSQLite(sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr), db, "mt commit put");
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
      auto key = "t" + std::to_string(tid) + "-k" + std::to_string(i);
      sqlite3_bind_text(get_stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
      RequireSQLite(sqlite3_step(get_stmt), db, "mt get step");
      sqlite3_reset(get_stmt);
    }
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();

    sqlite3_finalize(put_stmt);
    sqlite3_finalize(get_stmt);
    sqlite3_close(db);
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(n_threads, 0.0);
  std::vector<double> get_times(n_threads, 0.0);

  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < n_threads; ++t) {
    threads.emplace_back(worker, t, ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all)
          .count();

  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < n_threads; ++i) {
    worst_put_ms = std::max(worst_put_ms, put_times[i]);
    worst_get_ms = std::max(worst_get_ms, get_times[i]);
  }
  stats.put_ms = worst_put_ms;
  stats.get_ms = worst_get_ms;
  stats.update_ms = elapsed_total;
  stats.delete_ms = 0;

  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}
#endif  // DKV_HAVE_SQLITE
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
  const std::size_t kN = 1000000;
  const std::size_t kBatch = 5000;
  const std::size_t kThreads = 20;
  const std::size_t kOpsPerThread = 200000;

  std::cout << "Benchmarking " << kN << " ops\n";

  auto dkv_single = BenchDKVSingle(kN);
  std::cout << "\nDKV (single ops)\n";
  PrintStats("  ", kN, dkv_single);

  auto dkv_batch = BenchDKVBatch(kN, kBatch);
  std::cout << "\nDKV (batched writes, batch size " << kBatch << ")\n";
  PrintStats("  ", kN, dkv_batch);

  auto dkv_mt = BenchDKVMultithread(kThreads, kOpsPerThread);
  std::cout << "\nDKV (multithreaded, " << kThreads << " threads, " << kOpsPerThread << " ops each)\n";
  std::cout << "  put (worst thread): " << dkv_mt.put_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, dkv_mt.put_ms) << " ops/sec/thread)\n";
  std::cout << "  get (worst thread): " << dkv_mt.get_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, dkv_mt.get_ms) << " ops/sec/thread)\n";
  std::cout << "  total wall time (put+get): " << dkv_mt.update_ms << " ms  ("
            << OpsPerSec(kThreads * kOpsPerThread * 2, dkv_mt.update_ms) << " ops/sec total)\n";

#if DKV_HAVE_SQLITE
  auto sqlite_stats = BenchSQLite(kN, kBatch);
  std::cout << "\nSQLite (txn + prepared statements)\n";
  PrintStats("  ", kN, sqlite_stats);

  auto sqlite_mt = BenchSQLiteMultithread(kThreads, kOpsPerThread);
  std::cout << "\nSQLite (multithreaded, " << kThreads << " threads, " << kOpsPerThread << " ops each)\n";
  std::cout << "  put (worst thread): " << sqlite_mt.put_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, sqlite_mt.put_ms) << " ops/sec/thread)\n";
  std::cout << "  get (worst thread): " << sqlite_mt.get_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, sqlite_mt.get_ms) << " ops/sec/thread)\n";
  std::cout << "  total wall time (put+get): " << sqlite_mt.update_ms << " ms  ("
            << OpsPerSec(kThreads * kOpsPerThread * 2, sqlite_mt.update_ms) << " ops/sec total)\n";
#else
  std::cout << "\nSQLite bench skipped (DKV_HAVE_SQLITE=0)\n";
#endif
#if DKV_HAVE_LEVELDB
  auto leveldb_stats = BenchLevelDB(kN, kBatch);
  std::cout << "\nLevelDB (batched writes, batch size " << kBatch << ")\n";
  PrintStats("  ", kN, leveldb_stats);

  auto leveldb_mt = BenchLevelDBMultithread(kThreads, kOpsPerThread);
  std::cout << "\nLevelDB (multithreaded, " << kThreads << " threads, " << kOpsPerThread << " ops each)\n";
  std::cout << "  put (worst thread): " << leveldb_mt.put_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, leveldb_mt.put_ms) << " ops/sec/thread)\n";
  std::cout << "  get (worst thread): " << leveldb_mt.get_ms << " ms  ("
            << OpsPerSec(kOpsPerThread, leveldb_mt.get_ms) << " ops/sec/thread)\n";
  std::cout << "  total wall time (put+get): " << leveldb_mt.update_ms << " ms  ("
            << OpsPerSec(kThreads * kOpsPerThread * 2, leveldb_mt.update_ms) << " ops/sec total)\n";
#else
  std::cout << "\nLevelDB bench skipped (DKV_HAVE_LEVELDB=0)\n";
#endif

  return 0;
}
