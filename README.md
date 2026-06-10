# nfscache

Prototype shared-filesystem cache for Parquet data. The original target is NFS,
and the locking now also covers SMB/Windows shares (the lock-directory removal
and `mkdir` retry paths tolerate the racier removal semantics of SMB). The
SMB/Windows path has been validated end-to-end against a real share (a macOS
client over `mount_smbfs` against a Windows 10 Pro share): the concurrent
streaming swarm and the full locking suite — including stale reader/writer
recovery — pass over the mounted share, not just on local disk.

A cold load streams results from any slow source (for example Oracle) straight
into a Parquet cache file; a warm load returns the validated cache file. The
cache reads and writes Parquet directly with PyArrow.

## Install

```bash
uv add nfscache
```

The Oracle SQL source path requires the `oracledb` driver, available as the
`oracle` extra:

```bash
uv add nfscache[oracle]
```

## Usage

Create an `NFSCache` pointed at a directory on the shared filesystem and wrap
your cold-load function with `@nfscache.sql_parquet`. The wrapped function takes
a `connection` argument; the cache reuses that same connection to read the
source version (so a warm hit and a cold load share one connection — no separate
factory). The wrapped function only runs on a cache miss; on a warm hit the body
is skipped and the validated cache file is copied to the requested output path
through an atomic `*.part` + `os.replace` export. (Omit the `connection`
argument to disable SQL versioning and validate by parquet bytes/footer alone.)

```python
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.nfs_parquet_cache import NFSParquetCache

nfscache = NFSParquetCache(Path("__cache__/nfs"))


# The first argument is the SQL string; the second is the requested output
# path; the `connection` argument is reused by the cache to read the source
# version. The cache key comes from the normalized SQL, and the source version
# from MAX(ORA_ROWSCN) plus the row count of the detected FROM table.
@nfscache.sql_parquet
def stream(sql: str, parquet_path: Path, connection) -> None:
  table = pa.table({"example": [1, 2, 3]})  # replace with cursor.fetchmany()
  pq.write_table(table, parquet_path)


with oracledb.connect(
  user="SOMEUSER",
  password="cache",
  dsn="localhost:1521/FREEPDB1",
) as connection:
  stream(
    "select * from DEMO",
    Path("DEMO.parquet"),
    connection,
  )
```

See `nfscache/database/oracle_streaming.py` for a complete Oracle streaming
example that fetches with a cursor and writes batches via
`pyarrow.parquet.ParquetWriter`.

## Current Functionality

- Decorator API: `@nfscache.sql_parquet` — streams a SQL result set directly
  into a cached Parquet file.
- Writes cached Parquet with `pyarrow.parquet.ParquetWriter`; reads and
  validates it with `pyarrow.parquet`.
- Writes through unique `*.part` files, then atomically replaces the final file
  with `os.replace`.
- Exports results by copying the validated cache file to an output `*.part`
  file, then atomically replacing the requested output path.
- Cleans up partial cache files on write failure.
- Uses a per-cache-key mkdir-based read/write lock: warm readers create
  per-reader tokens and can overlap, while writers and invalidations block new
  readers and wait for active readers to finish.
- Lock tokens include `lock.json` metadata with hostname, PID, UUID,
  `created_at`, and `last_seen`; held locks heartbeat `last_seen`, and stale
  reader/writer tokens are broken after `stale_lock_seconds`.
- The default stale lock timeout is 15 minutes, sized for cold Oracle reads that
  can take around 10 minutes while still heartbeating as live work.
- Judges staleness in the file server's clock domain (via a measured
  client/server clock offset), so clock skew across hosts does not wrongly break
  or keep a lock.
- Reclaims a lock held by a dead process on the same host immediately (its PID no
  longer exists), without waiting out the timeout.
- Breaks a stale lock by atomic rename to a private name (only one racer wins),
  re-verifies staleness on what it grabbed, and restores it untouched if it
  turned out live — so a freshly recreated lock is never clobbered.
- Optional `acquire_timeout_seconds` bounds the total acquisition wait and raises
  `TimeoutError` instead of spinning forever on a wedged mount.
- Copies the validated cache file to the output path while still holding the read
  lock, so a concurrent writer cannot replace the file mid-copy.
- Adds an authoritative metadata sidecar:

```text
__cache__/nfs/sql/DEMO/<hash>.parquet
__cache__/nfs/sql/DEMO/<hash>.parquet.meta.json
```

- Metadata includes source key/version, parquet byte size, parquet SHA-256, row
  count, column count, schema hash, writer version, created time, and normalized
  `source_sql`.
- Readers reject missing, stale, unsupported, or corrupt metadata and validate
  parquet size, row count, and schema hash before returning a warm hit; the
  full-file SHA-256 is re-verified on read only when `verify_checksum=True` (off
  by default, since it re-reads the whole file on every warm hit).
- Invalidates stale cache entries when the source version changes.
- SQL sources use normalized SQL for cache keys and `COUNT(*)` plus
  `MAX(ORA_ROWSCN)` as the Oracle version token for the detected `FROM` table.
- Cold loads snapshot the source version once, before streaming, and store it
  with the entry; Oracle's statement-level read consistency means the streamed
  snapshot cannot be torn. A table that changes mid-stream just makes the next
  reader recompute a newer version and reload once — it never serves stale data.

## Swarm Test

`swarm_stream.py` tests the cache under process-level concurrency. It creates an
Oracle table, runs client processes that stream reads through
`@nfscache.sql_parquet`, and rewrites the table between read waves so the cache
has to invalidate and reload under load.

Start Oracle first, then run the default swarm:

```bash
uv run --no-cache --no-sync python -m nfscache.util.swarm_stream
```

Default behavior:

- 4 client processes
- 1 writer process
- 12 get waves
- 6 source regenerations injected throughout the get waves
- final multi-client warm check after all waves complete

Useful smaller run:

```bash
uv run --no-cache --no-sync python -m nfscache.util.swarm_stream \
  --clients 2 \
  --writers 1 \
  --gets-per-client 3 \
  --generations 2 \
  --n-rows 128 \
  --batch-size 64 \
  --table SWARM_STREAM_TEST \
  --out-dir /tmp/parquet-nfs-swarm-stream-out \
  --cache-dir /tmp/parquet-nfs-swarm-stream-cache
```

Swarm output includes Oracle cold streams, writer SCNs, stale SQL cache
invalidation, warm cache hits, and a final multi-client warm check.

## Tests

Run focused unit tests:

```bash
uv run --no-cache --no-sync python -m unittest discover -s tests
```

The tests cover authoritative metadata, corrupted metadata/parquet recovery,
normalized SQL metadata, overlapping warm readers, writer-preference locking,
and the Oracle pool/streaming helpers. A syntax check for all modules:

```bash
uv run --no-cache --no-sync python -m compileall -q nfscache tests
```

## Generate Parquets

Generate or replace test parquet files (built and written with PyArrow):

```bash
uv run --no-cache --no-sync python -m nfscache.util.generate_parquets
```

The generator writes to a unique `*.part` file and atomically replaces the final
parquet when the write is complete.

By default, content changes on every run. Use `--seed` for reproducible data:

```bash
uv run --no-cache --no-sync python -m nfscache.util.generate_parquets --seed 123
```

## Oracle SQL Cache

The Oracle demos run from a source checkout and need the `oracledb` driver. Sync
the `oracle` extra into the environment first:

```bash
uv sync --extra oracle
```

Start the local Oracle demo container:

```bash
./build_and_run.sh [--wipe]
```

Load a generated parquet file into an Oracle table (PyArrow → Oracle DDL +
`executemany`):

```bash
uv run --no-cache --no-sync python -m nfscache.database.oracle_write parquet/A_TEST_1048576.parquet
```

Stream Oracle SQL directly through the SQL Parquet cache:

```bash
uv run --no-cache --no-sync python -m nfscache.database.oracle_streaming \
  "select * from A_TEST_1048576" \
  A_TEST_1048576.parquet
```

`oracle_read.py` is a standalone Oracle smoke test (no cache): it reads a SQL
result set into a PyArrow `Table` and logs it.

```bash
uv run --no-cache --no-sync python -m nfscache.database.oracle_read "select * from A_TEST_1048576"
```

SQL cache keys use normalized SQL plus requested columns. Metadata stores the
normalized `source_sql`, and source versions use `COUNT(*)` plus
`MAX(ORA_ROWSCN)` for the detected `FROM` table. `@nfscache.sql_parquet`
stores the streamed Parquet file in the cache first, then atomically exports a
copy to the requested output path.

## Production Notes

This is not yet production-grade enterprise software.

The core lock and atomic-export primitives have been validated on a real SMB
share: a macOS `mount_smbfs` client against a Windows 10 Pro share ran the
streaming swarm (which committed a consistent cache) and the full locking suite
over the mount, including `mkdir` lock tokens, writer intent, stale reader/writer
recovery, and `os.replace`.

For Oracle on a shared filesystem (NFS, or an SMB/Windows share) with many
clients, the next important pieces are:

- validate the same primitives against a real NFS mount and against a mixed
  NFS-Linux + SMB-Windows client pool sharing one cache directory across
  multiple hosts (only a single SMB host has been exercised so far)
- tie long Oracle reads to a documented consistent SCN/snapshot strategy
- add structured logs and metrics for hit/miss/reload, reader/writer lock wait,
  cold load duration, parquet write/read duration, and corruption/retry counts
- broaden automated failure tests for crashed lock holders (same-host dead-owner
  recovery, clock-skew-safe staleness, and atomic stale-lock stealing now have
  unit coverage; cross-host crashes still need integration tests), corrupted
  files, source changes during cold load, and multi-host / multi-protocol
  integration (NFS and SMB clients against one cache)
- add operational controls for cache retention, quotas, old `*.part` cleanup,
  version migration, compression, permissions, and bad-key runbooks
```
