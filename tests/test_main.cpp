#include <cassert>
#include <chrono>
#include <filesystem>
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

}  // namespace

int main() {
  bool ok = true;
  ok &= TestBasicPutGet();
  ok &= TestFlushAndRecover();
  ok &= TestCompaction();
  ok &= TestBatchWrite();
   ok &= TestFuzzAgainstModel();
  if (!ok) {
    std::cerr << "Tests failed\n";
    return 1;
  }
  std::cout << "All tests passed\n";
  return 0;
}
