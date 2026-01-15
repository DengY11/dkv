# dkv-server

An embedded Redis-protocol server backed by `dkv` (this repo's embedded KV store), using a master/sub-reactor design:

- **Master reactor**: `epoll` only for `accept()`, then dispatches connection fds to sub-reactors.
- **Sub-reactors**: one `epoll` per thread, handles socket I/O + RESP parsing + task dispatch.
- **Worker pool**: runs `dkv` operations and posts responses back to the owning sub-reactor.

## Build

```bash
# from repo root
cmake -S . -B build -DDKV_BUILD_SERVER=ON
cmake --build build -j
```

## Run

```bash
./build/dkv-server --data-dir ./dkv-data --bind 0.0.0.0 --port 6379 --subreactors 4 --workers 8
```

Quick check with `redis-cli`:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET k v
redis-cli -p 6379 GET k
```

## Supported Commands (RESP2 subset)

- `PING [message]`
- `ECHO <message>`
- `GET <key>`
- `SET <key> <value>`
- `DEL <key> [key ...]`
- `EXISTS <key> [key ...]`
- `MGET <key> [key ...]`
- `MSET <key> <value> [key value ...]`
- `INCR <key>`
- `INCRBY <key> <increment>`
- `DECR <key>`
- `DECRBY <key> <decrement>`
- `QUIT`
- `HELLO` (minimal reply)
- `INFO` (minimal reply)
- `COMMAND` (returns empty array)
- `CLIENT SETINFO ...` (accepted; returns `OK`)
- `CONFIG GET <pattern>` (exposes server + dkv config, e.g. `CONFIG GET *`, `CONFIG GET data-dir`)
- `CONFIG RESETSTAT`, `CONFIG REWRITE` (returns `OK`)

Unsupported commands return `-ERR unknown command`.

## Configuration

All `dkv::Options` fields are exposed as CLI flags (see `include/dkv/options.h`).

Notes:

- `*-bytes` options accept plain integers (bytes) and also `K|KB`, `M|MB`, `G|GB` suffixes (base 1024).
- Boolean options accept `true/false/1/0/yes/no/on/off`. Passing the flag without a value means `true`.

### Server flags

- `--bind <ip>` (default `0.0.0.0`)
- `--port <port>` (default `6379`)
- `--subreactors <n>` (`0` = auto)
- `--workers <n>` (`0` = auto)

### dkv flags

- `--data-dir <path>`
- `--memtable-soft-limit-bytes <bytes>`
- `--sync-wal [bool]`
- `--sstable-target-size-bytes <bytes>`
- `--sstable-block-size-bytes <bytes>`
- `--bloom-bits-per-key <n>`
- `--bloom-cache-capacity-bytes <bytes>`
- `--raw-block-cache-capacity-bytes <bytes>`
- `--block-cache-capacity-bytes <bytes>`
- `--level0-file-limit <n>`
- `--level-base-bytes <bytes>`
- `--level-size-multiplier <n>`
- `--max-levels <n>`
- `--flush-thread-count <n>`
- `--max-immutable-memtables <n>`
- `--compaction-thread-count <n>`
- `--wal-sync-interval-ms <ms>`
- `--enable-crc [bool]`
- `--verify-sstable-crc [bool]`
- `--enable-compress [bool]` (requires `dkv` built with a compression backend)

## On-disk Layout

All persistent files live under `--data-dir`:

- `wal.log`: active WAL
- `wal-*.log`: rotated WAL segments
- `MANIFEST`: manifest file
- `sst/`: SSTable files
