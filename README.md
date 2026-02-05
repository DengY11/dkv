# dkv (C++20)

An embedded LSM-tree key-value store with an optional RESP server.

## Highlights
- WAL + MANIFEST for durability/recovery
- Memtable (skiplist + arena) with background flush pipeline
- SSTables with block index + Bloom filters, optional compression
- Block cache + raw block cache
- Background flush/compaction threads
- RESP server (Redis protocol subset) + header-only client

## Build & Install
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
sudo cmake --install build --prefix /usr/local
```

## Usage

### Embedded API
```cpp
#include <dkv/db.h>

int main() {
  dkv::Options opts;
  opts.data_dir = "my-data";

  std::unique_ptr<dkv::DB> db;
  dkv::DB::Open(opts, db);

  db->Put({}, "hello", "world");
  std::string value;
  db->Get({}, "hello", value);
}
```

### RESP client (server required)
```cpp
#include <dkv/resp_client.h>

int main() {
  dkv::RespClient c("127.0.0.1", 6379);
  c.kv().SetOk("k", "v");
  auto v = c.kv().GetString("k");
}
```

### Server
```bash
./build/dkv-server --data-dir ./dkv-data
```

## Build Options
- `DKV_BUILD_SERVER` (default ON): build `dkv-server`
- `DKV_BUILD_UTILS` (default ON): build `dkv-dump`
- `DKV_BUILD_TESTS` (default ON): build `dkv_tests` and `dkv-server-load-test`
- `DKV_BUILD_BENCHMARKS` (default ON): build benchmarks
- `DKV_BUILD_EXAMPLES` (default ON): build `dkv_example` and `dkv-resp-client-ops`

## Docs
- `docs/ARCHITECTURE.md`
- `docs/OPTIONS.md`
- `docs/COMPRESSION.md`
- `docs/BENCHMARKS.md`

## License
MIT (see LICENSE)
