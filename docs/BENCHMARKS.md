# Benchmark Guide

The benchmark driver lives in `benchmarks/bench.cc` and builds to `dkv_bench`. It exercises DKV / SQLite / LevelDB across sequential and random workloads, batched writes, multithreaded runs, and synchronous write modes.

## Build
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## Run
```bash
./build/dkv_bench \
  --n=1000000 \
  --batch=50000 \
  --threads=8 \
  --ops-per-thread=200000 \
  --sync-n=10000 \
  --dbs=dkv,sqlite,leveldb \
  --modes=seq,rand,batch,mt,sync-seq,sync-rand
```

### Parameters
- `--n`: total ops for single-threaded modes (put/get/update/delete). Default 1,000,000.
- `--batch`: batch size for batched modes. Default 50,000.
- `--threads`: thread count for multithreaded mode. Default 8.
- `--ops-per-thread`: ops per thread in multithreaded mode. Default 200,000. Alias `--ops-per-threads` is accepted.
- `--sync-n`: cap on ops for sync modes to avoid very long runs. Default 10,000.
- `--seed`: RNG seed. Default 12345.
- `--dbs`: comma-separated list of DBs to run (`dkv,sqlite,leveldb`).
- `--modes`: comma-separated list of modes:
  - `seq`: sequential put/get/update/delete
  - `rand`: random put/get/update/delete
  - `batch`: batched writes + sequential read/update/delete
  - `mt`: multithreaded put/get (per-thread unique keys)
  - `sync-seq`: sequential with sync writes (uses `--sync-n`)
  - `sync-rand`: random with sync writes (uses `--sync-n`)

Tip: for sync modes, keep `--n` small or set `--sync-n` to avoid long fsync-bound runs.

## Sample Results
Command:
```bash
./build/dkv_bench \
  --n=10000000 \
  --batch=50000 \
  --threads=20 \
  --ops-per-threads=500000 \
  --dbs=dkv,leveldb \
  --modes=seq,rand,batch,mt,sync-seq,sync-rand \
  --sync-n=1000
```

### RESULT
Intel E5-2698v4*2(40core 80threads) 128G(ddr4)
```bash
dkv benchmark suite
  n=1000000, batch=50000, threads=8, ops_per_thread=200000, sync_n=1000, seed=12345

DKV (seq, n=1000000)
   put:    1167 ms  (856898 ops/sec)
   get:    379 ms  (2.63852e+06 ops/sec)
   update: 1124 ms  (889680 ops/sec)
   delete: 1007 ms  (993049 ops/sec)

DKV (rand, n=1000000)
   put:    1025 ms  (975610 ops/sec)
   get:    375 ms  (2.66667e+06 ops/sec)
   update: 1169 ms  (855432 ops/sec)
   delete: 1097 ms  (911577 ops/sec)

DKV (batch, n=1000000)
   put:    694 ms  (1.44092e+06 ops/sec)
   get:    409 ms  (2.44499e+06 ops/sec)
   update: 670 ms  (1.49254e+06 ops/sec)
   delete: 670 ms  (1.49254e+06 ops/sec)

DKV (mt, 8 threads, 200000 ops each)
  put (worst thread): 3243 ms  (61671.3 ops/sec/thread)
  get (worst thread): 172 ms  (1.16279e+06 ops/sec/thread)
  total wall time (put+get): 3397 ms  (942008 ops/sec total)

DKV (sync-seq, n=1000)
   put:    18783 ms  (53.2396 ops/sec)
   get:    0 ms  (0 ops/sec)
   update: 19520 ms  (51.2295 ops/sec)
   delete: 18663 ms  (53.582 ops/sec)

DKV (sync-rand, n=1000)
   put:    18903 ms  (52.9017 ops/sec)
   get:    0 ms  (0 ops/sec)
   update: 18921 ms  (52.8513 ops/sec)
   delete: 18234 ms  (54.8426 ops/sec)

SQLite (seq, n=1000000)
   put:    1568 ms  (637755 ops/sec)
   get:    1124 ms  (889680 ops/sec)
   update: 2169 ms  (461042 ops/sec)
   delete: 1491 ms  (670691 ops/sec)

SQLite (rand, n=1000000)
   put:    6176 ms  (161917 ops/sec)
   get:    3234 ms  (309215 ops/sec)
   update: 17651 ms  (56654 ops/sec)
   delete: 8455 ms  (118273 ops/sec)

SQLite (batch, n=1000000)
   put:    1585 ms  (630915 ops/sec)
   get:    1050 ms  (952381 ops/sec)
   update: 2278 ms  (438982 ops/sec)
   delete: 1344 ms  (744048 ops/sec)

SQLite (mt, 8 threads, 200000 ops each)
  put (worst thread): 5197 ms  (38483.7 ops/sec/thread)
  get (worst thread): 250 ms  (800000 ops/sec/thread)
  total wall time (put+get): 5445 ms  (587695 ops/sec total)

SQLite (sync-seq, n=1000)
   put:    101 ms  (9900.99 ops/sec)
   get:    1 ms  (1e+06 ops/sec)
   update: 285 ms  (3508.77 ops/sec)
   delete: 84 ms  (11904.8 ops/sec)

SQLite (sync-rand, n=1000)
   put:    84 ms  (11904.8 ops/sec)
   get:    1 ms  (1e+06 ops/sec)
   update: 85 ms  (11764.7 ops/sec)
   delete: 81 ms  (12345.7 ops/sec)

LevelDB (seq, n=1000000)
   put:    2139 ms  (467508 ops/sec)
   get:    727 ms  (1.37552e+06 ops/sec)
   update: 2080 ms  (480769 ops/sec)
   delete: 2229 ms  (448632 ops/sec)

LevelDB (rand, n=1000000)
   put:    2780 ms  (359712 ops/sec)
   get:    1670 ms  (598802 ops/sec)
   update: 3352 ms  (298329 ops/sec)
   delete: 3676 ms  (272035 ops/sec)

LevelDB (batch, n=1000000)
   put:    698 ms  (1.43266e+06 ops/sec)
   get:    704 ms  (1.42045e+06 ops/sec)
   update: 789 ms  (1.26743e+06 ops/sec)
   delete: 840 ms  (1.19048e+06 ops/sec)

LevelDB (mt, 8 threads, 200000 ops each)
  put (worst thread): 10376 ms  (19275.3 ops/sec/thread)
  get (worst thread): 2015 ms  (99255.6 ops/sec/thread)
  total wall time (put+get): 12365 ms  (258795 ops/sec total)

LevelDB (sync-seq, n=1000)
   put:    22982 ms  (43.5123 ops/sec)
   get:    1 ms  (1e+06 ops/sec)
   update: 20706 ms  (48.2952 ops/sec)
   delete: 18490 ms  (54.0833 ops/sec)

LevelDB (sync-rand, n=1000)
   put:    19090 ms  (52.3834 ops/sec)
   get:    1 ms  (1e+06 ops/sec)
   update: 19491 ms  (51.3057 ops/sec)
   delete: 18558 ms  (53.8851 ops/sec)

```