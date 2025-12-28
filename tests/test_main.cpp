#include <cassert>
#include <chrono>
#include <filesystem>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "dkv/db.h"

namespace {

std::filesystem::path TempDir(const std::string& name) {
  auto base = std::filesystem::temp_directory_path();
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  std::mt19937_64 rng(now);
  std::uniform_int_distribution<std::uint64_t> dist;
  auto path = base / (name + "-" + std::to_string(dist(rng)));
  std::filesystem::create_directories(path);
  return path;
}

bool ExpectOk(const dkv::Status& s, const std::string& msg) {
  if (!s.ok()) {
    std::cerr << msg << ": " << s.ToString() << "\n";
    return false;
  }
  return true;
}

bool TestBasicPutGet() {
  auto dir = TempDir("dkv-basic");
  dkv::Options opts;
  opts.data_dir = dir;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;

  dkv::WriteOptions wopts;
  if (!ExpectOk(db->Put(wopts, "foo", "bar"), "put foo")) return false;
  if (!ExpectOk(db->Put(wopts, "hello", "world"), "put hello")) return false;

  std::string value;
  if (!ExpectOk(db->Get(dkv::ReadOptions{}, "foo", value), "get foo")) return false;
  assert(value == "bar");

  if (!ExpectOk(db->Delete(wopts, "foo"), "delete foo")) return false;
  auto s = db->Get(dkv::ReadOptions{}, "foo", value);
  assert(!s.ok() && s.code() == dkv::Status::Code::kNotFound);

  std::filesystem::remove_all(dir);
  return true;
}

bool TestFlushAndRecover() {
  auto dir = TempDir("dkv-recover");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;  // force flush quickly
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;

  dkv::WriteOptions wopts;
  for (int i = 0; i < 10; ++i) {
    if (!ExpectOk(db->Put(wopts, "k" + std::to_string(i), "v" + std::to_string(i)), "put loop")) {
      return false;
    }
  }
  if (!ExpectOk(db->Flush(), "flush")) return false;
  db.reset();

  std::unique_ptr<dkv::DB> db2;
  if (!ExpectOk(dkv::DB::Open(opts, db2), "reopen")) return false;

  std::string value;
  for (int i = 0; i < 10; ++i) {
    if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "k" + std::to_string(i), value), "get after reopen"))
      return false;
    assert(value == "v" + std::to_string(i));
  }

  std::filesystem::remove_all(dir);
  return true;
}

bool TestCompaction() {
  auto dir = TempDir("dkv-compact");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;

  dkv::WriteOptions wopts;
  if (!ExpectOk(db->Put(wopts, "alpha", "1"), "put alpha 1")) return false;
  if (!ExpectOk(db->Put(wopts, "beta", "2"), "put beta 2")) return false;
  if (!ExpectOk(db->Put(wopts, "alpha", "3"), "put alpha 3")) return false;
  if (!ExpectOk(db->Delete(wopts, "beta"), "delete beta")) return false;
  if (!ExpectOk(db->Flush(), "flush before compact")) return false;

  if (!ExpectOk(db->Compact(), "compact")) return false;
  std::string value;
  if (!ExpectOk(db->Get(dkv::ReadOptions{}, "alpha", value), "get alpha after compact")) return false;
  assert(value == "3");
  auto s = db->Get(dkv::ReadOptions{}, "beta", value);
  assert(!s.ok() && s.code() == dkv::Status::Code::kNotFound);

  std::filesystem::remove_all(dir);
  return true;
}

bool TestIteratorAndSnapshot() {
  auto dir = TempDir("dkv-iter");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db iter")) return false;

  dkv::WriteOptions wopts;
  db->Put(wopts, "a", "1");
  db->Put(wopts, "b", "2");
  db->Put(wopts, "c", "3");
  db->Put(wopts, "b", "2b");  // overwrite
  db->Delete(wopts, "a");     // tombstone should skip

  dkv::ReadOptions ropts;
  auto it = db->Scan(ropts);
  if (!it || !it->status().ok()) {
    std::cerr << "iterator init failed: " << (it ? it->status().ToString() : "null") << "\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  it->Seek("b");
  std::vector<std::pair<std::string, std::string>> iter_out;
  while (it->Valid()) {
    iter_out.emplace_back(std::string(it->key()), std::string(it->value()));
    it->Next();
  }
  if (iter_out.size() != 2 || iter_out[0].first != "b" || iter_out[0].second != "2b" ||
      iter_out[1].first != "c" || iter_out[1].second != "3") {
    std::cerr << "iterator unexpected results\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  // Prefix iterator: only keys starting with c
  auto it_pref = db->Scan(ropts, "c");
  it_pref->SeekToFirst();
  std::vector<std::pair<std::string, std::string>> pref_out;
  while (it_pref->Valid()) {
    pref_out.emplace_back(std::string(it_pref->key()), std::string(it_pref->value()));
    it_pref->Next();
  }
  if (pref_out.size() != 1 || pref_out[0].first != "c" || pref_out[0].second != "3") {
    std::cerr << "prefix iterator unexpected results\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  std::filesystem::remove_all(dir);
  return true;
}

bool TestSnapshotIsolationAndLargeScan() {
  auto dir = TempDir("dkv-iter-large");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 64 * 1024;
  opts.level0_file_limit = 1;  // force compaction when L0 has >1 file
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db iter-large")) return false;

  dkv::WriteOptions wopts;
  const int kTotal = 2000;
  for (int i = 0; i < kTotal; ++i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "k%06d", i);
    db->Put(wopts, buf, "v" + std::to_string(i));
  }
  if (!ExpectOk(db->Flush(), "flush stream-large")) {
    std::filesystem::remove_all(dir);
    return false;
  }

  // Snapshot iterator should be stable even after new writes.
  dkv::ReadOptions snap_opts;
  snap_opts.snapshot = true;
  auto snap_it = db->Scan(snap_opts);
  db->Put(dkv::WriteOptions{}, "k999999", "late");
  // Force compaction while snapshot is alive.
  if (!ExpectOk(db->Compact(), "compact with snapshot")) {
    std::filesystem::remove_all(dir);
    return false;
  }
  std::size_t snap_count = 0;
  while (snap_it->Valid()) {
    snap_count++;
    snap_it->Next();
  }
  if (snap_count != static_cast<std::size_t>(kTotal)) {
    std::cerr << "snapshot iter count mismatch " << snap_count << " expected " << kTotal << "\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  // Non-snapshot iterator should see the extra key.
  auto it = db->Scan(dkv::ReadOptions{});
  std::size_t count = 0;
  while (it->Valid()) {
    count++;
    it->Next();
  }
  if (count != static_cast<std::size_t>(kTotal + 1)) {
    std::cerr << "live iter count mismatch " << count << " expected " << kTotal + 1 << "\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  // Explicit snapshot_seq (earlier than latest) should exclude newer writes.
  dkv::ReadOptions past_opts;
  past_opts.snapshot_seq = snap_opts.snapshot_seq;  // same as earlier snapshot
  auto past_it = db->Scan(past_opts);
  std::size_t past_count = 0;
  while (past_it->Valid()) {
    past_count++;
    past_it->Next();
  }
  assert(past_count == snap_count);

  std::filesystem::remove_all(dir);
  return true;
}

bool TestCompressionOption() {
  auto dir = TempDir("dkv-compress");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;  // force blocks/flush
  opts.enable_compress = true;

  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db compress")) return false;

  dkv::WriteOptions wopts;
  if (!ExpectOk(db->Put(wopts, "c1", std::string(64, 'a')), "put c1")) return false;
  if (!ExpectOk(db->Put(wopts, "c2", std::string(128, 'b')), "put c2")) return false;
  if (!ExpectOk(db->Put(wopts, "c3", std::string(16, 'c')), "put c3")) return false;
  if (!ExpectOk(db->Flush(), "flush compress")) return false;
  db.reset();

  std::unique_ptr<dkv::DB> db2;
  if (!ExpectOk(dkv::DB::Open(opts, db2), "reopen compress")) return false;
  std::string value;
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c1", value), "get c1")) return false;
  assert(value == std::string(64, 'a'));
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c2", value), "get c2")) return false;
  assert(value == std::string(128, 'b'));
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c3", value), "get c3")) return false;
  assert(value == std::string(16, 'c'));

  std::filesystem::remove_all(dir);
  return true;
}

bool TestBatchWrite() {
  auto dir = TempDir("dkv-batch");
  dkv::Options opts;
  opts.data_dir = dir;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;

  dkv::WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("a");
  batch.Put("a", "3");
  batch.Delete("b");

  if (!ExpectOk(db->Write(dkv::WriteOptions{}, batch), "write batch")) return false;
  std::string value;
  if (!ExpectOk(db->Get(dkv::ReadOptions{}, "a", value), "get a batch")) return false;
  assert(value == "3");
  auto s = db->Get(dkv::ReadOptions{}, "b", value);
  assert(!s.ok() && s.code() == dkv::Status::Code::kNotFound);

  std::filesystem::remove_all(dir);
  return true;
}

std::string RandomString(std::mt19937_64& rng, std::size_t len) {
  static const char kChars[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<std::size_t> dist(0, sizeof(kChars) - 2);
  std::string s;
  s.reserve(len);
  for (std::size_t i = 0; i < len; ++i) s.push_back(kChars[dist(rng)]);
  return s;
}

bool TestFuzzAgainstModel() {
  auto dir = TempDir("dkv-fuzz");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 256 * 1024;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;

  std::mt19937_64 rng(123456789);
  std::uniform_int_distribution<int> op_dist(0, 2);  // 0=put,1=del,2=get
  std::uniform_int_distribution<int> key_len_dist(4, 16);
  std::uniform_int_distribution<int> val_len_dist(1, 32);

  // Model: key -> optional value (empty string represents tombstone)
  std::unordered_map<std::string, std::string> model;
  std::vector<std::string> key_space;
  key_space.reserve(200);
  for (int i = 0; i < 200; ++i) key_space.push_back(RandomString(rng, 8));
  std::uniform_int_distribution<std::size_t> key_pick(0, key_space.size() - 1);

  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  const int kOps = 5000;
  for (int i = 0; i < kOps; ++i) {
    const std::string& key = key_space[key_pick(rng)];
    int op = op_dist(rng);
    if (op == 0) {  // put
      std::string value = RandomString(rng, static_cast<std::size_t>(val_len_dist(rng)));
      if (!ExpectOk(db->Put(wopts, key, value), "fuzz put")) return false;
      model[key] = value;
    } else if (op == 1) {  // delete
      if (!ExpectOk(db->Delete(wopts, key), "fuzz delete")) return false;
      model.erase(key);
    } else {  // get
      std::string value;
      auto s = db->Get(ropts, key, value);
      auto it = model.find(key);
      if (it == model.end()) {
        if (s.ok()) {
          std::cerr << "fuzz get expected not found for key=" << key << "\n";
          return false;
        }
      } else {
        if (!s.ok() || value != it->second) {
          std::cerr << "fuzz get mismatch for key=" << key << " expected=" << it->second
                    << " got=" << value << " status=" << s.ToString() << "\n";
          return false;
        }
      }
    }

    // Occasionally flush/compact to exercise persistence path.
    if (i % 500 == 499) {
      if (!ExpectOk(db->Flush(), "fuzz flush")) return false;
      if (!ExpectOk(db->Compact(), "fuzz compact")) return false;
    }
  }

  // Final verification: full scan of model keys.
  for (const auto& kv : model) {
    std::string value;
    if (!ExpectOk(db->Get(ropts, kv.first, value), "final get")) return false;
    assert(value == kv.second);
  }

  std::filesystem::remove_all(dir);
  return true;
}

bool TestFuzzWithReopen() {
  auto dir = TempDir("dkv-fuzz-reopen");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 64 * 1024;  // trigger flushes
  opts.sync_wal = false;
  opts.wal_sync_interval_ms = 0;

  std::mt19937_64 rng(424242);
  std::uniform_int_distribution<int> op_dist(0, 2);   // 0=put,1=del,2=get
  std::uniform_int_distribution<int> key_len_dist(4, 12);
  std::uniform_int_distribution<int> val_len_dist(1, 24);

  // Persistent model across reopen.
  std::unordered_map<std::string, std::string> model;
  std::vector<std::string> key_space;
  key_space.reserve(256);
  for (int i = 0; i < 256; ++i) key_space.push_back(RandomString(rng, static_cast<std::size_t>(key_len_dist(rng))));
  std::uniform_int_distribution<std::size_t> key_pick(0, key_space.size() - 1);

  auto run_round = [&](int round_ops, int round_idx) -> bool {
    std::unique_ptr<dkv::DB> db;
    if (!ExpectOk(dkv::DB::Open(opts, db), "open db round " + std::to_string(round_idx))) return false;
    dkv::WriteOptions wopts;
    dkv::ReadOptions ropts;

    for (int i = 0; i < round_ops; ++i) {
      const std::string& key = key_space[key_pick(rng)];
      int op = op_dist(rng);
      if (op == 0) {
        std::string value = RandomString(rng, static_cast<std::size_t>(val_len_dist(rng)));
        if (!ExpectOk(db->Put(wopts, key, value), "fuzz reopen put")) return false;
        model[key] = value;
      } else if (op == 1) {
        if (!ExpectOk(db->Delete(wopts, key), "fuzz reopen delete")) return false;
        model.erase(key);
      } else {
        std::string value;
        auto s = db->Get(ropts, key, value);
        auto it = model.find(key);
        if (it == model.end()) {
          if (s.ok()) {
            std::cerr << "fuzz reopen get expected miss key=" << key << "\n";
            return false;
          }
        } else if (!s.ok() || value != it->second) {
          std::cerr << "fuzz reopen get mismatch key=" << key << " expected=" << it->second
                    << " got=" << value << " status=" << s.ToString() << "\n";
          return false;
        }
      }

      if (i % 200 == 199) {
        if (!ExpectOk(db->Flush(), "fuzz reopen flush")) return false;
        if (!ExpectOk(db->Compact(), "fuzz reopen compact")) return false;
      }
    }

    // Verify a sample before closing.
    for (int j = 0; j < 10; ++j) {
      const std::string& key = key_space[key_pick(rng)];
      std::string value;
      auto s = db->Get(ropts, key, value);
      auto it = model.find(key);
      if (it == model.end()) {
        if (s.ok()) {
          std::cerr << "fuzz reopen pre-close expected miss key=" << key << "\n";
          return false;
        }
      } else if (!s.ok() || value != it->second) {
        std::cerr << "fuzz reopen pre-close mismatch key=" << key << " expected=" << it->second
                  << " got=" << value << " status=" << s.ToString() << "\n";
        return false;
      }
    }

    db.reset();  // force reopen next round
    return true;
  };

  // multiple rounds with reopen between
  for (int round = 0; round < 3; ++round) {
    if (!run_round(800, round)) {
      std::filesystem::remove_all(dir);
      return false;
    }
  }

  // Final full validation after last reopen
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db final verify")) {
    std::filesystem::remove_all(dir);
    return false;
  }
  dkv::ReadOptions ropts;
  for (const auto& kv : model) {
    std::string value;
    if (!ExpectOk(db->Get(ropts, kv.first, value), "final verify get")) {
      std::filesystem::remove_all(dir);
      return false;
    }
    assert(value == kv.second);
  }

  std::filesystem::remove_all(dir);
  return true;
}

}  // namespace

int main() {
  bool ok = true;
  ok &= TestBasicPutGet();
  ok &= TestFlushAndRecover();
  ok &= TestCompaction();
  ok &= TestIteratorAndSnapshot();
  ok &= TestSnapshotIsolationAndLargeScan();
  ok &= TestCompressionOption();
  ok &= TestBatchWrite();
  ok &= TestFuzzAgainstModel();
  ok &= TestFuzzWithReopen();
  if (!ok) {
    std::cerr << "Tests failed\n";
    return 1;
  }
  std::cout << "All tests passed\n";
  return 0;
}
