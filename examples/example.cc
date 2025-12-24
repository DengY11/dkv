#include <iostream>
#include <string>
#include <vector>

#include "dkv/db.h"

int main() {
  dkv::Options opts;
  opts.data_dir = "dkv-example-data";
  opts.memtable_soft_limit_bytes = 64 * 1024 * 1024;
  opts.sync_wal = false;
  opts.wal_sync_interval_ms = 0;  // disable background sync for speed; set >0 for periodic durability

  std::unique_ptr<dkv::DB> db;
  dkv::Status s = dkv::DB::Open(opts, db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << "\n";
    return 1;
  }

  dkv::WriteOptions wopts;
  dkv::ReadOptions ropts;

  // Put a few keys.
  db->Put(wopts, "a", "1");
  db->Put(wopts, "b", "2");
  db->Put(wopts, "c", "3");

  // Get a key.
  std::string value;
  s = db->Get(ropts, "b", value);
  if (s.ok()) {
    std::cout << "b=" << value << "\n";
  } else {
    std::cout << "b not found: " << s.ToString() << "\n";
  }

  // Batch write.
  dkv::WriteBatch batch;
  batch.Put("d", "4");
  batch.Delete("a");
  db->Write(wopts, batch);

  // Scan from "b", up to 10 entries.
  std::vector<std::pair<std::string, std::string>> out;
  db->Scan(ropts, "b", 10, out);
  std::cout << "Scan from b:\n";
  for (const auto& [k, v] : out) {
    std::cout << "  " << k << " -> " << v << "\n";
  }

  // Show metrics.
  dkv::Metrics m = db->GetMetrics();
  std::cout << "Metrics: puts=" << m.puts << ", gets=" << m.gets << ", flushes=" << m.flushes
            << ", compactions=" << m.compactions << ", wal_syncs=" << m.wal_syncs << "\n";

  return 0;
}
