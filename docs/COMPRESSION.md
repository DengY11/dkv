# Compression Backends

DKV supports optional SSTable data-block compression. The compression *algorithm* is fixed at compile time, while enabling/disabling compression is a runtime option (`Options::enable_compress`).

## Choosing a backend (CMake)
- `-DDKV_COMPRESSION=auto` (default): pick the first available backend in order Snappy → Zstd → LZ4 → none.
- `-DDKV_COMPRESSION=snappy|zstd|lz4`: require the specified backend; CMake fails if the library is not found.
- `-DDKV_COMPRESSION=none`: build without any compression backend.

Examples:
```bash
cmake -S . -B build -DDKV_COMPRESSION=snappy
cmake --build build
```
If you want Zstd or LZ4 instead, swap the value accordingly.

## When compression is active
- At runtime, `Options::enable_compress` must be `true` for compression attempts.
- If the binary was built with `-DDKV_COMPRESSION=none` or no backend was detected in `auto`, compression silently stays disabled even if `enable_compress=true` (blocks are stored raw).
- If a backend is compiled in, compression is attempted; if a block fails to compress or does not shrink enough, the block is written uncompressed (matching LevelDB-style fallback).

## Verifying backend selection
The CMake configure log prints the chosen backend. You can also inspect the generated definitions:
- `DKV_USE_SNAPPY`, `DKV_USE_ZSTD`, `DKV_USE_LZ4` are set to `1`/`0` at compile time based on `DKV_COMPRESSION`.
