#include <cassert>
#include <cstdint>
#include <chrono>
#include <filesystem>
#include <fstream>
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

  db.reset();
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

  db2.reset();
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

  db.reset();
  std::filesystem::remove_all(dir);
  return true;
}

bool TestIteratorAndSnapshot() {
  auto dir = TempDir("dkv-iter");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;
  std::unique_ptr<dkv::DB> db;
  std::unique_ptr<dkv::DB::Iterator> it;
  std::unique_ptr<dkv::DB::Iterator> it_pref;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db iter")) return false;
  auto cleanup = [&]() {
    it.reset();
    it_pref.reset();
    db.reset();
    std::filesystem::remove_all(dir);
  };

  dkv::WriteOptions wopts;
  db->Put(wopts, "a", "1");
  db->Put(wopts, "b", "2");
  db->Put(wopts, "c", "3");
  db->Put(wopts, "b", "2b");  // overwrite
  db->Delete(wopts, "a");     // tombstone should skip

  dkv::ReadOptions ropts;
  it = db->Scan(ropts);
  if (!it || !it->status().ok()) {
    std::cerr << "iterator init failed: " << (it ? it->status().ToString() : "null") << "\n";
    cleanup();
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
    cleanup();
    return false;
  }

  // Prefix iterator: only keys starting with c
  it_pref = db->Scan(ropts, "c");
  it_pref->SeekToFirst();
  std::vector<std::pair<std::string, std::string>> pref_out;
  while (it_pref->Valid()) {
    pref_out.emplace_back(std::string(it_pref->key()), std::string(it_pref->value()));
    it_pref->Next();
  }
  if (pref_out.size() != 1 || pref_out[0].first != "c" || pref_out[0].second != "3") {
    std::cerr << "prefix iterator unexpected results\n";
    cleanup();
    return false;
  }

  cleanup();
  return true;
}

bool TestSnapshotIsolationAndLargeScan() {
  auto dir = TempDir("dkv-iter-large");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 4 * 1024 * 1024;
  opts.level0_file_limit = 1;  // force compaction when L0 has >1 file
  std::unique_ptr<dkv::DB> db;
  std::unique_ptr<dkv::DB::Iterator> snap_it;
  std::unique_ptr<dkv::DB::Iterator> it;
  std::unique_ptr<dkv::DB::Iterator> past_it;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db iter-large")) return false;
  auto cleanup = [&]() {
    snap_it.reset();
    it.reset();
    past_it.reset();
    db.reset();
    std::filesystem::remove_all(dir);
  };

  dkv::WriteOptions wopts;
  const int kTotal = 2000;
  for (int i = 0; i < kTotal; ++i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "k%06d", i);
    db->Put(wopts, buf, "v" + std::to_string(i));
  }
  if (!ExpectOk(db->Flush(), "flush stream-large")) {
    cleanup();
    return false;
  }

  // Snapshot iterator should be stable even after new writes.
  dkv::ReadOptions snap_opts;
  snap_opts.snapshot = true;
  snap_it = db->Scan(snap_opts);
  db->Put(dkv::WriteOptions{}, "k999999", "late");
  // Force compaction while snapshot is alive.
  if (!ExpectOk(db->Compact(), "compact with snapshot")) {
    cleanup();
    return false;
  }
  std::size_t snap_count = 0;
  while (snap_it->Valid()) {
    snap_count++;
    snap_it->Next();
  }
  if (snap_count != static_cast<std::size_t>(kTotal)) {
    std::cerr << "snapshot iter count mismatch " << snap_count << " expected " << kTotal << "\n";
    cleanup();
    return false;
  }

  // Non-snapshot iterator should see the extra key.
  it = db->Scan(dkv::ReadOptions{});
  std::size_t count = 0;
  while (it->Valid()) {
    count++;
    it->Next();
  }
  if (count != static_cast<std::size_t>(kTotal + 1)) {
    std::cerr << "live iter count mismatch " << count << " expected " << kTotal + 1 << "\n";
    cleanup();
    return false;
  }

  // Explicit snapshot_seq (earlier than latest) should exclude newer writes.
  dkv::ReadOptions past_opts;
  past_opts.snapshot_seq = snap_opts.snapshot_seq;  // same as earlier snapshot
  past_it = db->Scan(past_opts);
  std::size_t past_count = 0;
  while (past_it->Valid()) {
    past_count++;
    past_it->Next();
  }
  assert(past_count == snap_count);

  cleanup();
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
  auto cleanup = [&]() {
    db.reset();
    db2.reset();
    std::filesystem::remove_all(dir);
  };
  std::string value;
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c1", value), "get c1")) {
    cleanup();
    return false;
  }
  assert(value == std::string(64, 'a'));
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c2", value), "get c2")) {
    cleanup();
    return false;
  }
  assert(value == std::string(128, 'b'));
  if (!ExpectOk(db2->Get(dkv::ReadOptions{}, "c3", value), "get c3")) {
    cleanup();
    return false;
  }
  assert(value == std::string(16, 'c'));

  cleanup();
  return true;
}

bool TestBlockBloomFilters() {
  auto dir = TempDir("dkv-block-bloom");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;
  opts.block_bloom_bits_per_key = 8;

  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db block bloom")) return false;
  dkv::WriteOptions wopts;
  for (int i = 0; i < 3; ++i) {
    db->Put(wopts, "b" + std::to_string(i), "v" + std::to_string(i));
  }
  if (!ExpectOk(db->Flush(), "flush block bloom")) return false;
  db.reset();

  std::filesystem::path sst_path;
  auto sst_dir = dir / "sst";
  for (const auto& entry : std::filesystem::directory_iterator(sst_dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      sst_path = entry.path();
      break;
    }
  }
  if (sst_path.empty()) {
    std::cerr << "no sstable found for block bloom\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  auto read32 = [](std::istream& in, std::uint32_t& v) -> bool {
    char buf[4];
    if (!in.read(buf, 4)) return false;
    v = static_cast<std::uint32_t>(static_cast<unsigned char>(buf[0])) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[1])) << 8) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[2])) << 16) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(buf[3])) << 24);
    return true;
  };
  auto read64 = [](std::istream& in, std::uint64_t& v) -> bool {
    char buf[8];
    if (!in.read(buf, 8)) return false;
    v = 0;
    for (int i = 7; i >= 0; --i) {
      v <<= 8u;
      v |= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[i]));
    }
    return true;
  };

  std::ifstream in(sst_path, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "failed to open sstable\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  in.seekg(0, std::ios::end);
  auto file_size = static_cast<std::uint64_t>(in.tellg());
  constexpr std::uint64_t footer_size = sizeof(std::uint64_t) * 5 + sizeof(std::uint32_t) * 2;
  if (file_size < footer_size) {
    std::cerr << "sstable too small\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  in.seekg(static_cast<std::streamoff>(file_size - footer_size));
  std::uint64_t index_start = 0, bloom_start = 0, filter_start = 0, filter_index_start = 0, max_seq = 0;
  std::uint32_t block_count = 0, magic = 0;
  if (!read64(in, index_start) || !read64(in, bloom_start) || !read64(in, filter_start) ||
      !read64(in, filter_index_start) || !read64(in, max_seq) || !read32(in, block_count) || !read32(in, magic)) {
    std::cerr << "failed to read footer\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  if (magic != 0xD15EEDu || filter_start == 0 || filter_index_start == 0 || block_count == 0) {
    std::cerr << "block filter footer invalid\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  in.seekg(static_cast<std::streamoff>(filter_start));
  std::uint32_t bpk = 0, k = 0;
  if (!read32(in, bpk) || !read32(in, k) || bpk == 0 || k == 0) {
    std::cerr << "block filter header invalid\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  in.seekg(static_cast<std::streamoff>(filter_index_start));
  std::vector<std::uint64_t> offsets(block_count, 0);
  for (std::uint32_t i = 0; i < block_count; ++i) {
    if (!read64(in, offsets[i])) {
      std::cerr << "block filter index invalid\n";
      std::filesystem::remove_all(dir);
      return false;
    }
  }
  if (offsets[0] == 0) {
    std::cerr << "first block filter offset missing\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  in.seekg(static_cast<std::streamoff>(offsets[0]));
  std::uint32_t filter_bytes = 0;
  if (!read32(in, filter_bytes) || filter_bytes == 0) {
    std::cerr << "block filter length invalid\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  if (!ExpectOk(dkv::DB::Open(opts, db), "reopen block bloom db")) {
    std::filesystem::remove_all(dir);
    return false;
  }
  std::string value;
  if (!ExpectOk(db->Get(dkv::ReadOptions{}, "b0", value), "get block bloom key")) {
    std::filesystem::remove_all(dir);
    return false;
  }
  assert(value == "v0");
  db.reset();
  std::filesystem::remove_all(dir);
  return true;
}

bool TestSSTableCrcStats() {
  auto dir = TempDir("dkv-crc-stats");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 32;  // small to force a tiny SST
  opts.verify_sstable_crc = true;

  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db crc stats")) return false;

  dkv::WriteOptions wopts;
  if (!ExpectOk(db->Put(wopts, "c-key", "value1"), "put crc key")) return false;
  if (!ExpectOk(db->Flush(), "flush crc")) return false;
  db.reset();  // close before corrupting file

  auto sst_dir = dir / "sst";
  std::filesystem::path sst_path;
  if (std::filesystem::exists(sst_dir)) {
    for (const auto& entry : std::filesystem::directory_iterator(sst_dir)) {
      if (entry.is_regular_file() && entry.path().extension() == ".sst") {
        sst_path = entry.path();
        break;
      }
    }
  }
  if (sst_path.empty()) {
    std::cerr << "no sstable found to corrupt\n";
    std::filesystem::remove_all(dir);
    return false;
  }

  // Flip one byte in the first block payload to trigger CRC failure.
  std::fstream f(sst_path, std::ios::in | std::ios::out | std::ios::binary);
  if (!f.is_open()) {
    std::cerr << "failed to open sstable for corruption\n";
    std::filesystem::remove_all(dir);
    return false;
  }
  constexpr std::streamoff kPayloadOffset = 4 + 4 + 1 + 4;  // header bytes
  f.seekg(0, std::ios::end);
  const auto size = f.tellg();
  if (size <= kPayloadOffset) {
    std::cerr << "sstable too small to corrupt\n";
    f.close();
    std::filesystem::remove_all(dir);
    return false;
  }
  f.seekg(kPayloadOffset);
  char byte = 0;
  f.read(&byte, 1);
  if (!f) {
    std::cerr << "failed to read payload byte\n";
    f.close();
    std::filesystem::remove_all(dir);
    return false;
  }
  byte ^= static_cast<char>(0xFF);
  f.seekp(kPayloadOffset);
  f.write(&byte, 1);
  f.flush();
  f.close();

  // Reopen and trigger a read to increment counters.
  if (!ExpectOk(dkv::DB::Open(opts, db), "reopen after corruption")) {
    std::filesystem::remove_all(dir);
    return false;
  }
  std::string value;
  auto s = db->Get(dkv::ReadOptions{}, "c-key", value);
  if (s.ok()) {
    std::cerr << "expected get to fail after corruption\n";
    db.reset();
    std::filesystem::remove_all(dir);
    return false;
  }
  dkv::Metrics m = db->GetMetrics();
  db.reset();
  std::filesystem::remove_all(dir);
  if (m.sstable_crc_errors == 0 || m.sstable_read_errors == 0) {
    std::cerr << "expected crc/read error counters to increment, got crc=" << m.sstable_crc_errors
              << " read=" << m.sstable_read_errors << "\n";
    return false;
  }
  return true;
}

bool TestBatchWrite() {
  auto dir = TempDir("dkv-batch");
  dkv::Options opts;
  opts.data_dir = dir;
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db")) return false;
  auto cleanup = [&]() {
    db.reset();
    std::filesystem::remove_all(dir);
  };

  dkv::WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("a");
  batch.Put("a", "3");
  batch.Delete("b");

  if (!ExpectOk(db->Write(dkv::WriteOptions{}, batch), "write batch")) {
    cleanup();
    return false;
  }
  std::string value;
  if (!ExpectOk(db->Get(dkv::ReadOptions{}, "a", value), "get a batch")) {
    cleanup();
    return false;
  }
  assert(value == "3");
  auto s = db->Get(dkv::ReadOptions{}, "b", value);
  assert(!s.ok() && s.code() == dkv::Status::Code::kNotFound);

  cleanup();
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
  auto cleanup = [&]() {
    db.reset();
    std::filesystem::remove_all(dir);
  };

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

  cleanup();
  return true;
}

bool TestFuzzWithReopen() {
  auto dir = TempDir("dkv-fuzz-reopen");
  dkv::Options opts;
  opts.data_dir = dir;
  opts.memtable_soft_limit_bytes = 64 * 1024;  // trigger flushes
  opts.sync_wal = false;
  opts.wal_sync_interval_ms = 0;
  auto cleanup_dir = [&]() { std::filesystem::remove_all(dir); };

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
      cleanup_dir();
      return false;
    }
  }

  // Final full validation after last reopen
  std::unique_ptr<dkv::DB> db;
  if (!ExpectOk(dkv::DB::Open(opts, db), "open db final verify")) {
    cleanup_dir();
    return false;
  }
  dkv::ReadOptions ropts;
  for (const auto& kv : model) {
    std::string value;
    if (!ExpectOk(db->Get(ropts, kv.first, value), "final verify get")) {
      db.reset();
      cleanup_dir();
      return false;
    }
    assert(value == kv.second);
  }

  db.reset();
  cleanup_dir();
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
  ok &= TestBlockBloomFilters();
  ok &= TestSSTableCrcStats();
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
