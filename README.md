# parquet_nfs_db_cache

Prototype NFS-backed cache for `DataContainer` objects whose payload is a
Polars `DataFrame`.

The cache stores container data as Parquet on a shared filesystem. Cold loads
can read from any slow source, for example Oracle, MySQL, or a local parquet
file. Warm loads use `polars.read_parquet`.

## Current Functionality

- Decorator API: `@dbcache.data_container_cache`.
- Stores `DataContainer.data.rows_data_pl` as a Parquet cache file.
- Reads cached objects with the fast Polars parquet reader.
- Writes cached objects with `pyarrow.parquet.ParquetWriter`.
- Uses a per-cache-key directory lock so multiple clients do not write the
  same cache file at once.
- Writes through unique `*.part` files, then atomically replaces the final file
  with `os.replace`.
- Cleans up partial cache files on write failure.
- Adds a metadata sidecar:

```text
__cache__/nfs/parquet/A_TEST_1048576.parquet
__cache__/nfs/parquet/A_TEST_1048576.parquet.meta.json
```

- Invalidates stale cache entries when the source version changes.
- For file path arguments, the default source version is a SHA-256 content hash.
- For database sources, pass a custom source version provider, for example an
  Oracle SCN.

```python
from pathlib import Path

from nfs_cache import DBCache

dbcache = DBCache(Path("__cache__/nfs"), source_version=lambda query_key, scn: scn)
```

## Demo

Run:

```bash
uv run -m main
```

`main.py` runs:

1. generate parquet source data
2. cold load and write cache
3. warm cache load
4. regenerate parquet source data
5. reload because the source hash changed
6. warm cache load again

Expected shape:

```text
Generating: parquet/A_TEST_1048576.parquet...
Reading: parquet/A_TEST_1048576.parquet...
Returning cached object: parquet/A_TEST_1048576.parquet sha=<first 40 chars>...
Generating: parquet/A_TEST_1048576.parquet...
Reading: parquet/A_TEST_1048576.parquet...
Returning cached object: parquet/A_TEST_1048576.parquet sha=<first 40 chars>...
```

## Swarm Test

`swarm.py` tests a multi-client environment with process-level concurrency.
It mixes cache gets with source regeneration to simulate clients reading while
the source data changes.

Run the default swarm:

```bash
uv run -m swarm
```

Default behavior:

- 4 client processes
- 12 get waves
- 6 source regenerations
- generations are injected throughout the get waves
- final warm check after all waves complete

Useful smaller run:

```bash
uv run -m swarm \
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

## Generate Parquets

Generate or replace test parquet files:

```bash
uv run python -m nfs_cache.util.generate_parquets
```

The generator writes to a unique `*.part` file and atomically replaces the final
parquet when the write is complete.

By default, content changes on every run. Use `--seed` for reproducible data:

```bash
uv run python -m nfs_cache.util.generate_parquets --seed 123
```

## Production Notes

This is not yet production-grade enterprise software.

For Oracle on NFS with many clients, the next important pieces are:

- use an Oracle SCN or equivalent stable version token for invalidation
- add stale lock detection and recovery for crashed writers
- validate `mkdir` locking and `os.replace` semantics on the actual NFS mount
- add structured logs and metrics for hit/miss/reload/lock wait/write duration
- add automated failure tests for crashed writers, corrupted cache files,
  corrupted metadata, and source changes during cold load
