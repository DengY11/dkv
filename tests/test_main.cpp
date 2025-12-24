#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
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

}  // namespace

int main() {
  bool ok = true;
  ok &= TestBasicPutGet();
  ok &= TestFlushAndRecover();
  ok &= TestCompaction();
  ok &= TestBatchWrite();
  if (!ok) {
    std::cerr << "Tests failed\n";
    return 1;
  }
  std::cout << "All tests passed\n";
  return 0;
}
