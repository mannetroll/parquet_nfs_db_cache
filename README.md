# parquet_nfs_db_cache

Prototype NFS-backed cache for `DataContainer` objects whose payload is a
Polars `DataFrame`.

The cache stores container data as Parquet on a shared filesystem. Cold loads
can read from any slow source, for example Oracle, MySQL, or a local parquet
file. Warm loads use `polars.read_parquet`.

## Current Functionality

- Decorator API: `@dbcache.data_container_cache` and `@nfscache.sql`.
- Stores `DataContainer.data.rows_data_pl` as a Parquet cache file.
- Reads cached objects with the fast Polars parquet reader.
- Writes cached objects with `pyarrow.parquet.ParquetWriter`.
- Writes through unique `*.part` files, then atomically replaces the final file
  with `os.replace`.
- Cleans up partial cache files on write failure.
- Uses a per-cache-key mkdir-based read/write lock: warm readers create
  per-reader tokens and can overlap, while writers and invalidations block new
  readers and wait for active readers to finish.
- Lock tokens include `lock.json` metadata with hostname, PID, UUID,
  `created_at`, and `last_seen`; held locks heartbeat `last_seen`, and stale
  reader/writer tokens are broken after `stale_lock_seconds`.
- Adds an authoritative metadata sidecar:

```text
__cache__/nfs/parquet/A_TEST_1048576.parquet
__cache__/nfs/parquet/A_TEST_1048576.parquet.meta.json
```

- Metadata includes source key/version, parquet byte size, parquet SHA-256, row
  count, column count, schema hash, writer version, created time, and normalized
  `source_sql` for SQL-backed entries.
- Readers reject missing, stale, unsupported, or corrupt metadata and validate
  parquet size/checksum/row count/schema hash before returning a warm hit.
- Invalidates stale cache entries when the source version changes.
- For file path arguments, the default source version is a SHA-256 content hash.
- SQL sources use normalized SQL for cache keys and `COUNT(*)` plus
  `MAX(ORA_ROWSCN)` as the Oracle version token for the detected `FROM` table.
- Cold loads re-read the source version before and after loading and retry if
  the source changes during the read.

## Demo

Run:

```bash
uv run --no-cache --no-sync python -m main
```

`main.py` runs:

1. clear `__cache__`
2. generate parquet source data
3. cold load and write cache
4. warm cache load
5. regenerate parquet source data
6. reload because the source hash changed
7. warm cache load again

Expected shape:

```text
Clearing cache: __cache__
Generating: parquet/A_TEST_1048576.parquet...
Reading: parquet/A_TEST_1048576.parquet...
Returning cached object: parquet/A_TEST_1048576.parquet sha=<first 40 chars>...
Generating: parquet/A_TEST_1048576.parquet...
Ignoring cache entry: parquet/A_TEST_1048576.parquet: stale source version
Reading: parquet/A_TEST_1048576.parquet...
Returning cached object: parquet/A_TEST_1048576.parquet sha=<first 40 chars>...
```

## Swarm Test

`swarm.py` tests a multi-client environment with process-level concurrency.
It mixes cache gets with source regeneration to simulate clients reading while
the source data changes.

Run the default swarm:

```bash
uv run --no-cache --no-sync python -m swarm
```

Default behavior:

- 4 client processes
- 12 get waves
- 6 source regenerations
- generations are injected throughout the get waves
- final warm check after all waves complete

Useful smaller run:

```bash
uv run --no-cache --no-sync python -m swarm \
  --clients 3 \
  --generators 1 \
  --gets-per-client 6 \
  --generations 3 \
  --n-rows 1024 \
  --cols 6 \
  --n-int-cols 2 \
  --n-str-cols 1 \
  --data-dir /tmp/parquet-nfs-wave-swarm-parquet \
  --cache-dir /tmp/parquet-nfs-wave-swarm-cache
```

Swarm output includes:

- source generation hash
- cold `Reading: ...` reloads after invalidation
- warm `Returning cached object: ... sha=...` hits
- final multi-client warm check

## Tests

Run focused unit tests:

```bash
uv run --no-cache --no-sync python -m unittest discover -s tests
```

The tests cover authoritative metadata, corrupted metadata/parquet recovery,
normalized SQL metadata, overlapping warm readers, and writer-preference
locking. A syntax check for all modules:

```bash
uv run --no-cache --no-sync python -m compileall -q disk_cache database tests main.py swarm.py
```

## Generate Parquets

Generate or replace test parquet files:

```bash
uv run --no-cache --no-sync python -m disk_cache.util.generate_parquets
```

The generator writes to a unique `*.part` file and atomically replaces the final
parquet when the write is complete.

By default, content changes on every run. Use `--seed` for reproducible data:

```bash
uv run --no-cache --no-sync python -m disk_cache.util.generate_parquets --seed 123
```

## Oracle SQL Cache

Start the local Oracle demo container:

```bash
./build_and_run.sh [--wipe]
```

Populate the demo table:

```bash
uv run --no-cache --no-sync python -m database.oracle_write_container
```

Read through the SQL cache:

```bash
uv run --no-cache --no-sync python -m database.oracle_read "select * from DATA_CONTAINER_DEMO"
```

SQL cache keys use normalized SQL plus requested columns. Metadata stores the
normalized `source_sql`, and source versions use `COUNT(*)` plus
`MAX(ORA_ROWSCN)` for the detected `FROM` table.

## Production Notes

This is not yet production-grade enterprise software.

For Oracle on NFS with many clients, the next important pieces are:

- validate `mkdir` lock tokens, writer intent, stale-lock recovery, and
  `os.replace` semantics on the actual NFS mount
- tie long Oracle reads to a documented consistent SCN/snapshot strategy
- add structured logs and metrics for hit/miss/reload, reader/writer lock wait,
  cold load duration, parquet write/read duration, and corruption/retry counts
- broaden automated failure tests for crashed lock holders, corrupted files,
  source changes during cold load, and multi-host NFS integration
- add operational controls for cache retention, quotas, old `*.part` cleanup,
  version migration, compression, permissions, and bad-key runbooks
