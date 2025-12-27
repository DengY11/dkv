#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <random>
#include <sstream>
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

enum class Mode { kSeq, kRand, kBatch, kMt, kSyncSeq, kSyncRand };

struct Args {
  std::size_t n{1'000'000};
  std::size_t batch{50'000};
  std::size_t threads{8};
  std::size_t ops_per_thread{200'000};
  std::size_t sync_n{1000};
  std::uint64_t seed{12345};
  std::vector<std::string> dbs{"dkv", "sqlite", "leveldb"};
  std::vector<Mode> modes{Mode::kSeq, Mode::kRand, Mode::kBatch, Mode::kMt, Mode::kSyncSeq, Mode::kSyncRand};
};

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

std::mt19937_64 MakeRng(std::uint64_t seed, const std::string& tag) {
  return std::mt19937_64(seed ^ static_cast<std::uint64_t>(std::hash<std::string>{}(tag)));
}

void FillIdsSeq(std::vector<std::size_t>& ids, std::size_t start, std::size_t count) {
  ids.resize(count);
  for (std::size_t i = 0; i < count; ++i) ids[i] = start + i;
}

void FillIdsRand(std::vector<std::size_t>& ids, std::size_t count, std::size_t n, std::mt19937_64& rng) {
  ids.resize(count);
  std::uniform_int_distribution<std::size_t> dist(0, n - 1);
  for (std::size_t i = 0; i < count; ++i) ids[i] = dist(rng);
}

void FillKeysFromIds(std::vector<std::string>& keys, const std::vector<std::size_t>& ids, const std::string& prefix) {
  keys.resize(ids.size());
  for (std::size_t i = 0; i < ids.size(); ++i) {
    keys[i] = prefix + std::to_string(ids[i]);
  }
}

void FillValuesFromIds(std::vector<std::string>& values, const std::vector<std::size_t>& ids, char prefix) {
  values.resize(ids.size());
  for (std::size_t i = 0; i < ids.size(); ++i) {
    values[i] = std::string(1, prefix) + std::to_string(ids[i]);
  }
}

void FillThreadKeys(std::vector<std::string>& keys, std::size_t tid, std::size_t start, std::size_t count) {
  keys.resize(count);
  for (std::size_t i = 0; i < count; ++i) {
    keys[i] = "t" + std::to_string(tid) + "-k" + std::to_string(start + i);
  }
}

CrudStats BenchDKVSingle(const Args& args, Mode mode) {
  const bool use_batch = mode == Mode::kBatch;
  const bool random = mode == Mode::kRand || mode == Mode::kSyncRand;
  const bool sync = mode == Mode::kSyncSeq || mode == Mode::kSyncRand;

  CrudStats stats;
  auto dir = TempDir("dkv-suite");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 512 * 1024 * 1024;
  opts.sync_wal = sync;
  opts.sstable_block_size_bytes = 16 * 1024;
  opts.bloom_bits_per_key = 8;
  opts.level0_file_limit = 8;
  opts.sstable_target_size_bytes = 16 * 1024 * 1024;
  opts.wal_sync_interval_ms = sync ? 0 : 5;
  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv");

  dkv::WriteOptions wopts;
  wopts.sync = sync;
  dkv::ReadOptions ropts;

  std::vector<std::size_t> ids;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  auto rng_put = MakeRng(args.seed, "dkv-put-" + std::to_string(static_cast<int>(mode)));
  auto rng_get = MakeRng(args.seed, "dkv-get-" + std::to_string(static_cast<int>(mode)));
  auto rng_upd = MakeRng(args.seed, "dkv-upd-" + std::to_string(static_cast<int>(mode)));
  auto rng_del = MakeRng(args.seed, "dkv-del-" + std::to_string(static_cast<int>(mode)));

  auto run_loop = [&](char val_prefix, std::mt19937_64& rng, auto&& func) {
    for (std::size_t i = 0; i < args.n; i += args.batch) {
      const auto count = std::min(args.batch, args.n - i);
      if (random) {
        FillIdsRand(ids, count, args.n, rng);
      } else {
        FillIdsSeq(ids, i, count);
      }
      FillKeysFromIds(keys, ids, "key");
      FillValuesFromIds(values, ids, val_prefix);
      func(count);
    }
  };

  auto start_put = std::chrono::steady_clock::now();
  run_loop('v', rng_put, [&](std::size_t count) {
    if (use_batch) {
      dkv::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Put(keys[j], values[j]);
      EnsureOk(db->Write(wopts, batch), "dkv batch put");
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        EnsureOk(db->Put(wopts, keys[j], values[j]), "dkv put");
      }
    }
  });
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  run_loop('v', rng_get, [&](std::size_t count) {
    (void)count;
    for (std::size_t j = 0; j < keys.size(); ++j) {
      auto s = db->Get(ropts, keys[j], value);
      if (!s.ok()) {
        if (random && s.code() == dkv::Status::Code::kNotFound) continue;
        EnsureOk(s, "dkv get");
      }
    }
  });
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  run_loop('u', rng_upd, [&](std::size_t count) {
    if (use_batch) {
      dkv::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Put(keys[j], values[j]);
      EnsureOk(db->Write(wopts, batch), "dkv batch update");
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        EnsureOk(db->Put(wopts, keys[j], values[j]), "dkv update");
      }
    }
  });
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  run_loop('x', rng_del, [&](std::size_t count) {
    if (use_batch) {
      dkv::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Delete(keys[j]);
      EnsureOk(db->Write(wopts, batch), "dkv batch delete");
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        EnsureOk(db->Delete(wopts, keys[j]), "dkv delete");
      }
    }
  });
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  db.reset();
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}

CrudStats BenchDKVMt(const Args& args) {
  CrudStats stats;
  auto dir = TempDir("dkv-suite-mt");

  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 512 * 1024 * 1024;
  opts.sync_wal = false;
  opts.wal_sync_interval_ms = 0;
  std::unique_ptr<dkv::DB> db;
  EnsureOk(dkv::DB::Open(opts, db), "open dkv mt");

  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  auto worker = [&](std::size_t tid, std::size_t count, double& put_ms, double& get_ms) {
    std::vector<std::string> keys;
    auto start_put = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        EnsureOk(db->Put(wopts, keys[j], "v" + std::to_string(i + j)), "mt put");
      }
    }
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        EnsureOk(db->Get(ropts, keys[j], value), "mt get");
      }
    }
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(args.threads, 0.0);
  std::vector<double> get_times(args.threads, 0.0);
  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < args.threads; ++t) {
    threads.emplace_back(worker, t, args.ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all).count();

  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < args.threads; ++i) {
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

#if DKV_HAVE_LEVELDB
CrudStats BenchLevelDBMt(const Args& args) {
  CrudStats stats;
  auto dir = TempDir("leveldb-suite-mt");

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.compression = leveldb::kNoCompression;
  opts.block_size = 16 * 1024;
  opts.write_buffer_size = 512 * 1024 * 1024;
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

  leveldb::WriteOptions lw;
  lw.sync = false;

  auto worker = [&](std::size_t tid, std::size_t count, double& put_ms, double& get_ms) {
    std::vector<std::string> keys;
    auto start_put = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        auto s = db->Put(lw, keys[j], "v" + std::to_string(i + j));
        if (!s.ok()) {
          std::cerr << "leveldb mt put: " << s.ToString() << "\n";
          std::exit(1);
        }
      }
    }
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        auto s = db->Get(leveldb::ReadOptions(), keys[j], &value);
        if (!s.ok()) {
          std::cerr << "leveldb mt get: " << s.ToString() << "\n";
          std::exit(1);
        }
      }
    }
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(args.threads, 0.0);
  std::vector<double> get_times(args.threads, 0.0);
  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < args.threads; ++t) {
    threads.emplace_back(worker, t, args.ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all).count();

  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < args.threads; ++i) {
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

CrudStats BenchLevelDB(const Args& args, Mode mode) {
  const bool use_batch = mode == Mode::kBatch;
  const bool random = mode == Mode::kRand || mode == Mode::kSyncRand;
  const bool sync = mode == Mode::kSyncSeq || mode == Mode::kSyncRand;

  CrudStats stats;
  auto dir = TempDir("leveldb-suite");

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.compression = leveldb::kNoCompression;
  opts.block_size = 16 * 1024;
  opts.write_buffer_size = 512 * 1024 * 1024;
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

  leveldb::WriteOptions lw;
  lw.sync = sync;
  std::vector<std::size_t> ids;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  auto rng_put = MakeRng(args.seed, "ldb-put-" + std::to_string(static_cast<int>(mode)));
  auto rng_get = MakeRng(args.seed, "ldb-get-" + std::to_string(static_cast<int>(mode)));
  auto rng_upd = MakeRng(args.seed, "ldb-upd-" + std::to_string(static_cast<int>(mode)));
  auto rng_del = MakeRng(args.seed, "ldb-del-" + std::to_string(static_cast<int>(mode)));

  auto run_loop = [&](char val_prefix, std::mt19937_64& rng, auto&& func) {
    for (std::size_t i = 0; i < args.n; i += args.batch) {
      const auto count = std::min(args.batch, args.n - i);
      if (random) {
        FillIdsRand(ids, count, args.n, rng);
      } else {
        FillIdsSeq(ids, i, count);
      }
      FillKeysFromIds(keys, ids, "key");
      FillValuesFromIds(values, ids, val_prefix);
      func(count);
    }
  };

  auto start_put = std::chrono::steady_clock::now();
  run_loop('v', rng_put, [&](std::size_t count) {
    if (use_batch) {
      leveldb::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Put(keys[j], values[j]);
      auto s = db->Write(lw, &batch);
      if (!s.ok()) {
        std::cerr << "leveldb batch put: " << s.ToString() << "\n";
        std::exit(1);
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        auto s = db->Put(lw, keys[j], values[j]);
        if (!s.ok()) {
          std::cerr << "leveldb put: " << s.ToString() << "\n";
          std::exit(1);
        }
      }
    }
  });
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  run_loop('v', rng_get, [&](std::size_t count) {
    (void)count;
    for (std::size_t j = 0; j < keys.size(); ++j) {
      auto s = db->Get(leveldb::ReadOptions(), keys[j], &value);
      if (!s.ok() && !(random && s.IsNotFound())) {
        std::cerr << "leveldb get: " << s.ToString() << "\n";
        std::exit(1);
      }
    }
  });
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  run_loop('u', rng_upd, [&](std::size_t count) {
    if (use_batch) {
      leveldb::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Put(keys[j], values[j]);
      auto s = db->Write(lw, &batch);
      if (!s.ok()) {
        std::cerr << "leveldb batch update: " << s.ToString() << "\n";
        std::exit(1);
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        auto s = db->Put(lw, keys[j], values[j]);
        if (!s.ok()) {
          std::cerr << "leveldb update: " << s.ToString() << "\n";
          std::exit(1);
        }
      }
    }
  });
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  run_loop('x', rng_del, [&](std::size_t count) {
    if (use_batch) {
      leveldb::WriteBatch batch;
      for (std::size_t j = 0; j < count; ++j) batch.Delete(keys[j]);
      auto s = db->Write(lw, &batch);
      if (!s.ok()) {
        std::cerr << "leveldb batch delete: " << s.ToString() << "\n";
        std::exit(1);
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        auto s = db->Delete(lw, keys[j]);
        if (!s.ok()) {
          std::cerr << "leveldb delete: " << s.ToString() << "\n";
          std::exit(1);
        }
      }
    }
  });
  stats.delete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_delete)
                        .count();

  db.reset();
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  return stats;
}
#endif  // DKV_HAVE_LEVELDB

#if DKV_HAVE_SQLITE
bool CheckSQLite(int rc, sqlite3* db, const char* msg) {
  if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
    std::cerr << msg << ": " << (db ? sqlite3_errmsg(db) : "unknown") << "\n";
    return false;
  }
  return true;
}

void RequireSQLite(int rc, sqlite3* db, const char* msg) {
  if (!CheckSQLite(rc, db, msg)) std::exit(1);
}

void ExecWithRetry(sqlite3* db, const char* sql, const char* msg, int max_retries = 100,
                   std::chrono::microseconds backoff = std::chrono::microseconds(1000)) {
  int rc = SQLITE_OK;
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) break;
    std::this_thread::sleep_for(backoff);
  }
  RequireSQLite(rc, db, msg);
}

template <typename BindFn>
void StepWithRetry(sqlite3* db, sqlite3_stmt* stmt, const char* msg, BindFn binder, int max_retries = 100,
                   std::chrono::microseconds backoff = std::chrono::microseconds(1000)) {
  int rc = SQLITE_OK;
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    binder();
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) break;
    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    std::this_thread::sleep_for(backoff);
  }
  RequireSQLite(rc, db, msg);
  sqlite3_reset(stmt);
  sqlite3_clear_bindings(stmt);
}

CrudStats BenchSQLiteMt(const Args& args) {
  CrudStats stats;
  auto dir = TempDir("sqlite-suite-mt");
  auto db_path = dir / "bench.db";

  {
    sqlite3* db = nullptr;
    RequireSQLite(sqlite3_open(db_path.string().c_str(), &db), db, "mt init open");
    RequireSQLite(sqlite3_busy_timeout(db, 10000), db, "mt init busy timeout");
    RequireSQLite(sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr), db, "mt init wal");
    RequireSQLite(sqlite3_exec(db, "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr), db, "mt init sync");
    RequireSQLite(sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);", nullptr, nullptr, nullptr),
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

    std::vector<std::string> keys;
    auto start_put = std::chrono::steady_clock::now();
    ExecWithRetry(db, "BEGIN IMMEDIATE;", "mt begin put");
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        StepWithRetry(db, put_stmt, "mt sqlite put", [&] {
          sqlite3_bind_text(put_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
          sqlite3_bind_text(put_stmt, 2, ("v" + std::to_string(i + j)).c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    }
    ExecWithRetry(db, "COMMIT;", "mt commit put");
    put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_put)
                 .count();

    std::string value;
    auto start_get = std::chrono::steady_clock::now();
    ExecWithRetry(db, "BEGIN;", "mt begin get");
    for (std::size_t i = 0; i < count; i += args.batch) {
      const auto chunk = std::min(args.batch, count - i);
      FillThreadKeys(keys, tid, i, chunk);
      for (std::size_t j = 0; j < chunk; ++j) {
        StepWithRetry(db, get_stmt, "mt sqlite get", [&] {
          sqlite3_bind_text(get_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
        });
        (void)value;
      }
    }
    ExecWithRetry(db, "COMMIT;", "mt commit get");
    get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - start_get)
                 .count();

    sqlite3_finalize(put_stmt);
    sqlite3_finalize(get_stmt);
    sqlite3_close(db);
  };

  std::vector<std::thread> threads;
  std::vector<double> put_times(args.threads, 0.0);
  std::vector<double> get_times(args.threads, 0.0);
  auto start_all = std::chrono::steady_clock::now();
  for (std::size_t t = 0; t < args.threads; ++t) {
    threads.emplace_back(worker, t, args.ops_per_thread, std::ref(put_times[t]), std::ref(get_times[t]));
  }
  for (auto& th : threads) th.join();
  auto elapsed_total =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_all).count();

  double worst_put_ms = 0, worst_get_ms = 0;
  for (std::size_t i = 0; i < args.threads; ++i) {
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

CrudStats BenchSQLite(const Args& args, Mode mode) {
  const bool use_batch = mode == Mode::kBatch;
  const bool random = mode == Mode::kRand || mode == Mode::kSyncRand;
  const bool sync = mode == Mode::kSyncSeq || mode == Mode::kSyncRand;

  CrudStats stats;
  auto dir = TempDir("sqlite-suite");
  auto db_path = dir / "bench.db";

  sqlite3* db = nullptr;
  RequireSQLite(sqlite3_open(db_path.string().c_str(), &db), db, "open sqlite");
  RequireSQLite(sqlite3_busy_timeout(db, 10000), db, "busy timeout");
  RequireSQLite(sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr), db, "pragma wal");
  RequireSQLite(sqlite3_exec(db, sync ? "PRAGMA synchronous=FULL;" : "PRAGMA synchronous=OFF;", nullptr, nullptr, nullptr),
                db, "pragma sync");
  RequireSQLite(sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);", nullptr, nullptr, nullptr),
                db, "create table");

  sqlite3_stmt* put_stmt = nullptr;
  sqlite3_stmt* get_stmt = nullptr;
  sqlite3_stmt* del_stmt = nullptr;
  RequireSQLite(sqlite3_prepare_v2(db, "REPLACE INTO kv(k,v) VALUES(?,?);", -1, &put_stmt, nullptr), db,
                "prepare put");
  RequireSQLite(sqlite3_prepare_v2(db, "SELECT v FROM kv WHERE k=?;", -1, &get_stmt, nullptr), db, "prepare get");
  RequireSQLite(sqlite3_prepare_v2(db, "DELETE FROM kv WHERE k=?;", -1, &del_stmt, nullptr), db, "prepare delete");

  std::vector<std::size_t> ids;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  auto rng_put = MakeRng(args.seed, "sqlite-put-" + std::to_string(static_cast<int>(mode)));
  auto rng_get = MakeRng(args.seed, "sqlite-get-" + std::to_string(static_cast<int>(mode)));
  auto rng_upd = MakeRng(args.seed, "sqlite-upd-" + std::to_string(static_cast<int>(mode)));
  auto rng_del = MakeRng(args.seed, "sqlite-del-" + std::to_string(static_cast<int>(mode)));

  auto run_loop = [&](char val_prefix, std::mt19937_64& rng, auto&& func) {
    for (std::size_t i = 0; i < args.n; i += args.batch) {
      const auto count = std::min(args.batch, args.n - i);
      if (random) {
        FillIdsRand(ids, count, args.n, rng);
      } else {
        FillIdsSeq(ids, i, count);
      }
      FillKeysFromIds(keys, ids, "key");
      FillValuesFromIds(values, ids, val_prefix);
      func(count);
    }
  };

  auto start_put = std::chrono::steady_clock::now();
  ExecWithRetry(db, "BEGIN IMMEDIATE;", "begin put");
  run_loop('v', rng_put, [&](std::size_t count) {
    if (use_batch) {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, put_stmt, "sqlite put", [&] {
          sqlite3_bind_text(put_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
          sqlite3_bind_text(put_stmt, 2, values[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, put_stmt, "sqlite put", [&] {
          sqlite3_bind_text(put_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
          sqlite3_bind_text(put_stmt, 2, values[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    }
  });
  ExecWithRetry(db, "COMMIT;", "commit put");
  stats.put_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_put)
                     .count();

  std::string value;
  auto start_get = std::chrono::steady_clock::now();
  ExecWithRetry(db, "BEGIN;", "begin get");
  run_loop('v', rng_get, [&](std::size_t count) {
    (void)count;
    for (std::size_t j = 0; j < keys.size(); ++j) {
      StepWithRetry(db, get_stmt, "sqlite get", [&] {
        sqlite3_bind_text(get_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
      });
    }
  });
  ExecWithRetry(db, "COMMIT;", "commit get");
  stats.get_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start_get)
                     .count();

  auto start_update = std::chrono::steady_clock::now();
  ExecWithRetry(db, "BEGIN IMMEDIATE;", "begin update");
  run_loop('u', rng_upd, [&](std::size_t count) {
    if (use_batch) {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, put_stmt, "sqlite update", [&] {
          sqlite3_bind_text(put_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
          sqlite3_bind_text(put_stmt, 2, values[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, put_stmt, "sqlite update", [&] {
          sqlite3_bind_text(put_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
          sqlite3_bind_text(put_stmt, 2, values[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    }
  });
  ExecWithRetry(db, "COMMIT;", "commit update");
  stats.update_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_update)
                        .count();

  auto start_delete = std::chrono::steady_clock::now();
  ExecWithRetry(db, "BEGIN IMMEDIATE;", "begin delete");
  run_loop('x', rng_del, [&](std::size_t count) {
    if (use_batch) {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, del_stmt, "sqlite delete", [&] {
          sqlite3_bind_text(del_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    } else {
      for (std::size_t j = 0; j < count; ++j) {
        StepWithRetry(db, del_stmt, "sqlite delete", [&] {
          sqlite3_bind_text(del_stmt, 1, keys[j].c_str(), -1, SQLITE_TRANSIENT);
        });
      }
    }
  });
  ExecWithRetry(db, "COMMIT;", "commit delete");
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

double OpsPerSec(std::size_t count, double ms) {
  if (ms <= 0.0) return 0.0;
  return static_cast<double>(count) / (ms / 1000.0);
}

std::vector<std::string> Split(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

bool ParseArgs(int argc, char** argv, Args& args) {
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    auto pos = arg.find('=');
    auto key = arg.substr(0, pos);
    auto val = (pos == std::string::npos) ? "" : arg.substr(pos + 1);
    if (key == "--n" && !val.empty()) {
      args.n = static_cast<std::size_t>(std::stoull(val));
    } else if (key == "--batch" && !val.empty()) {
      args.batch = static_cast<std::size_t>(std::stoull(val));
    } else if (key == "--threads" && !val.empty()) {
      args.threads = static_cast<std::size_t>(std::stoull(val));
    } else if ((key == "--ops-per-thread" || key == "--ops-per-threads") && !val.empty()) {
      args.ops_per_thread = static_cast<std::size_t>(std::stoull(val));
    } else if (key == "--sync-n" && !val.empty()) {
      args.sync_n = static_cast<std::size_t>(std::stoull(val));
    } else if (key == "--seed" && !val.empty()) {
      args.seed = static_cast<std::uint64_t>(std::stoull(val));
    } else if (key == "--dbs" && !val.empty()) {
      args.dbs = Split(val);
    } else if (key == "--modes" && !val.empty()) {
      args.modes.clear();
      for (const auto& m : Split(val)) {
        if (m == "seq") args.modes.push_back(Mode::kSeq);
        else if (m == "rand") args.modes.push_back(Mode::kRand);
        else if (m == "batch") args.modes.push_back(Mode::kBatch);
        else if (m == "mt") args.modes.push_back(Mode::kMt);
        else if (m == "sync-seq") args.modes.push_back(Mode::kSyncSeq);
        else if (m == "sync-rand") args.modes.push_back(Mode::kSyncRand);
      }
    } else if (key == "--help") {
      std::cout << "Usage: ./dkv_bench_suite [--n=1e6] [--batch=50000] [--threads=8] "
                   "[--ops-per-thread=200000] [--sync-n=1000] [--seed=12345] "
                   "[--dbs=dkv,sqlite,leveldb] [--modes=seq,rand,batch,mt,sync-seq,sync-rand]\n";
      return false;
    }
  }
  return true;
}

void PrintStats(const std::string& label, std::size_t n, const CrudStats& s) {
  std::cout << label << " put:    " << s.put_ms << " ms  (" << OpsPerSec(n, s.put_ms) << " ops/sec)\n";
  std::cout << label << " get:    " << s.get_ms << " ms  (" << OpsPerSec(n, s.get_ms) << " ops/sec)\n";
  std::cout << label << " update: " << s.update_ms << " ms  (" << OpsPerSec(n, s.update_ms) << " ops/sec)\n";
  std::cout << label << " delete: " << s.delete_ms << " ms  (" << OpsPerSec(n, s.delete_ms) << " ops/sec)\n";
}

std::string ModeName(Mode m) {
  switch (m) {
    case Mode::kSeq:
      return "seq";
    case Mode::kRand:
      return "rand";
    case Mode::kBatch:
      return "batch";
    case Mode::kMt:
      return "mt";
    case Mode::kSyncSeq:
      return "sync-seq";
    case Mode::kSyncRand:
      return "sync-rand";
  }
  return "unknown";
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!ParseArgs(argc, argv, args)) return 0;

  std::cout << "dkv benchmark suite\n";
  std::cout << "  n=" << args.n << ", batch=" << args.batch << ", threads=" << args.threads
            << ", ops_per_thread=" << args.ops_per_thread << ", sync_n=" << args.sync_n
            << ", seed=" << args.seed << "\n";

  auto ModeArgs = [&](Mode m) {
    Args out = args;
    if (m == Mode::kSyncSeq || m == Mode::kSyncRand) {
      out.n = std::min(args.n, args.sync_n);
    }
    return out;
  };

  for (const auto& db_name : args.dbs) {
    bool run_dkv = (db_name == "dkv");
    bool run_sqlite = (db_name == "sqlite");
    bool run_leveldb = (db_name == "leveldb");

    if (run_dkv) {
      for (auto mode : args.modes) {
        if (mode == Mode::kMt) {
          auto s = BenchDKVMt(args);
          std::cout << "\nDKV (" << ModeName(mode) << ", " << args.threads << " threads, "
                    << args.ops_per_thread << " ops each)\n";
          std::cout << "  put (worst thread): " << s.put_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.put_ms) << " ops/sec/thread)\n";
          std::cout << "  get (worst thread): " << s.get_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.get_ms) << " ops/sec/thread)\n";
          std::cout << "  total wall time (put+get): " << s.update_ms << " ms  ("
                    << OpsPerSec(args.threads * args.ops_per_thread * 2, s.update_ms) << " ops/sec total)\n";
        } else {
          auto a = ModeArgs(mode);
          auto s = BenchDKVSingle(a, mode);
          std::cout << "\nDKV (" << ModeName(mode) << ", n=" << a.n << ")\n";
          PrintStats("  ", a.n, s);
        }
      }
    }

#if DKV_HAVE_SQLITE
    if (run_sqlite) {
      for (auto mode : args.modes) {
        if (mode == Mode::kMt) {
          auto s = BenchSQLiteMt(args);
          std::cout << "\nSQLite (" << ModeName(mode) << ", " << args.threads << " threads, "
                    << args.ops_per_thread << " ops each)\n";
          std::cout << "  put (worst thread): " << s.put_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.put_ms) << " ops/sec/thread)\n";
          std::cout << "  get (worst thread): " << s.get_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.get_ms) << " ops/sec/thread)\n";
          std::cout << "  total wall time (put+get): " << s.update_ms << " ms  ("
                    << OpsPerSec(args.threads * args.ops_per_thread * 2, s.update_ms) << " ops/sec total)\n";
        } else {
          auto a = ModeArgs(mode);
          auto s = BenchSQLite(a, mode);
          std::cout << "\nSQLite (" << ModeName(mode) << ", n=" << a.n << ")\n";
          PrintStats("  ", a.n, s);
        }
      }
    }
#else
    if (run_sqlite) {
      std::cout << "\nSQLite requested but DKV_HAVE_SQLITE=0; skipping.\n";
    }
#endif

#if DKV_HAVE_LEVELDB
    if (run_leveldb) {
      for (auto mode : args.modes) {
        if (mode == Mode::kMt) {
          auto s = BenchLevelDBMt(args);
          std::cout << "\nLevelDB (" << ModeName(mode) << ", " << args.threads << " threads, "
                    << args.ops_per_thread << " ops each)\n";
          std::cout << "  put (worst thread): " << s.put_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.put_ms) << " ops/sec/thread)\n";
          std::cout << "  get (worst thread): " << s.get_ms << " ms  ("
                    << OpsPerSec(args.ops_per_thread, s.get_ms) << " ops/sec/thread)\n";
          std::cout << "  total wall time (put+get): " << s.update_ms << " ms  ("
                    << OpsPerSec(args.threads * args.ops_per_thread * 2, s.update_ms) << " ops/sec total)\n";
        } else {
          auto a = ModeArgs(mode);
          auto s = BenchLevelDB(a, mode);
          std::cout << "\nLevelDB (" << ModeName(mode) << ", n=" << a.n << ")\n";
          PrintStats("  ", a.n, s);
        }
      }
    }
#else
    if (run_leveldb) {
      std::cout << "\nLevelDB requested but DKV_HAVE_LEVELDB=0; skipping.\n";
    }
#endif
  }

  return 0;
}
