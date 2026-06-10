# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A prototype shared-filesystem cache for Parquet data. A cold load streams results from any
slow source (Oracle, etc.) straight into a Parquet cache file; a warm load returns the
validated cache file. The cache reads and writes Parquet with PyArrow.
The design target is an Oracle-on-NFS deployment
with many concurrent clients, so most of the complexity is about making concurrent writes and
cache invalidation safe over a shared filesystem (NFS, and now SMB/Windows shares).

## Commands

This project uses `uv` (Python 3.13). Dependencies live in `pyproject.toml` / `uv.lock`.
Runtime deps are `numpy` and `pyarrow`; Oracle is an optional extra (`oracledb`).

```bash
uv sync
uv run --no-cache --no-sync python -m nfscache.util.swarm_stream
uv run --no-cache --no-sync python -m unittest discover -s tests
uv run --no-cache --no-sync python -m compileall -q nfscache tests
uv run --no-cache --no-sync python -m nfscache.util.generate_parquets [--seed N]
```

`swarm_stream.py` takes flags to size the run (`--clients`, `--writers`, `--gets-per-client`,
`--generations`, `--n-rows`, `--batch-size`, `--table`, `--out-dir`, `--cache-dir`) and requires
Oracle; see README for a small example.

The project has focused `unittest` coverage under `tests/` for metadata integrity, SQL
invalidation, and locking, plus the Oracle pool/streaming helpers. There is no linter or
formatter configured.

> **Running uv inside Claude Code's command sandbox:** plain `uv run` fails with
> `Failed to initialize cache ... .git: Operation not permitted` because the sandbox blocks `.git`
> files (uv's package cache contains some). Use `uv run --no-cache --no-sync ...` (the env is already
> synced in `.venv`). This is a sandbox workaround, not a `UV_CACHE_DIR` override. Do **not** set
> `UV_CACHE_DIR`. Connecting to local Oracle on `localhost:1521` works from the sandbox.
> Note: `ProcessPoolExecutor` (used by `swarm_stream.py`) cannot allocate semaphores in the
> sandbox, so the swarm only runs fully outside it; table setup still confirms the Oracle path.

### Oracle (optional, for the database source path)

```bash
./build_and_run.sh [--wipe]   # build + run gvenzl/oracle-free in Docker on :1521 (service FREEPDB1)
```

`init/001_create_user_and_privs.sql` runs once on first DB init, creating user `SOMEUSER`/`cache`
and granting `SELECT ON V_$DATABASE` (needed for `current_scn`). Oracle connection settings are read
from CLI flags, then overridden by a `.env` file (see `nfscache/database/oracle_env.py`, keys `ORACLE_HOST`,
`ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_TABLE`, `ORACLE_BATCH_SIZE`).
The `.env` file wins when present.

```bash
uv run --no-cache --no-sync python -m nfscache.database.oracle_write parquet/A_TEST_1048576.parquet
uv run --no-cache --no-sync python -m nfscache.database.oracle_streaming "select * from A_TEST_1048576" out.parquet
uv run --no-cache --no-sync python -m nfscache.database.oracle_read "select * from A_TEST_1048576"
```

`oracle_streaming` goes *through the cache*: a miss streams from Oracle and writes the cache file,
a hit logs `Returning cached object: sql/<TABLE>/<hash>.parquet version=<TABLE>@SCN:<scn>|ROWS:<n>`.
To see a full cold -> warm -> reload cycle, run it twice, then `oracle_write` (advances the SCN),
then run it again. `oracle_read` is a standalone Oracle smoke test (no cache): it reads SQL into a
PyArrow `Table` and logs it.

## Architecture

### Core: `nfscache/nfs_cache.py` — `NFSCache`

The caching engine is one class exposing **one decorator**, `@nfscache.sql_parquet`, that wraps a
`Callable[..., None]` whose job is to write a Parquet file to a given path. The wrapped function is
the cold-load source; the decorator handles locking, invalidation, the streaming write, and the
atomic export (the export runs while the cache lock is still held, so the file cannot be replaced
mid-copy). It funnels into `_run_cached_parquet(display_key, version_fn, write_fn, export_fn)`.

- `@nfscache.sql_parquet` — first arg is the SQL string (optional `return_cols=` kwarg); a path
  argument named `parquet_path` or `output_path` receives the destination. Key is
  `sql/<TABLE>/<sha256(normalized_sql|cols)>.parquet`; version is read from Oracle as
  `<TABLE>@SCN:<MAX(ORA_ROWSCN)>|ROWS:<count>` via `_sql_source_version`, using
  `NFSCache.connect_factory`. The table is parsed from the SQL with `_FROM_RE`. On a cache miss the
  wrapped function is called with the path replaced by a `*.part` file; on a hit the validated cache
  file is copied to the requested output path.

`connect_factory` is an opaque `Callable[[], connection]` set on the `NFSCache` instance (see
`nfscache/database/oracle_streaming.py` and `swarm_stream.py`, which assign a pooled factory). It is
kept generic so `nfs_cache.py` never imports `oracledb`. If unset, SQL versioning is disabled and
entries are validated by parquet size/footer (and the SHA-256 only when `verify_checksum=True`).

Key mechanics to understand before changing:

- **Cache key / path** (`_sql_display_key` + `_cache_path`): the normalized SQL (plus optional
  `return_cols`) hashes into `sql/<TABLE>/<hash>.parquet` under `cache_dir`. Absolute paths and paths
  containing `..` are hashed into `_absolute/` or `_relative/` subdirs.
- **Locking** (`_acquire_read_lock` + `_acquire_write_lock`): a per-cache-key directory read/write
  lock via `mkdir` (atomic on NFS; resilient removal + `mkdir` retry for SMB/Windows). Warm readers
  create per-reader tokens and can overlap. Writers create a writer-intent directory, block new
  readers, and wait for active readers to drain before writing or revalidating stale entries. Reader
  tokens and writer intent store `lock.json` metadata (`hostname`, `pid`, `uuid`, `created_at`,
  `last_seen`) and update `last_seen` by heartbeat while held. The default `stale_lock_seconds` is
  15 minutes so a live 10-minute Oracle cold read is not treated as abandoned; an optional
  `acquire_timeout_seconds` bounds the total wait so a wedged mount raises `TimeoutError` instead of
  spinning forever.
- **Stale-lock recovery** (`_break_stale_lock` + `_is_stale_lock`): staleness is judged in the file
  server's clock domain — the cache learns its offset from the server clock via the mtimes the server
  stamps on files it writes, so client/server clock skew does not wrongly break (or keep) a lock. A
  lock held by a dead process on the *same host* (its `pid` no longer exists) is reclaimed immediately,
  without waiting out the timeout. A stale lock is broken by *atomic rename* to a unique private name
  (the only compare-and-swap a shared FS offers): exactly one racer wins, then it re-verifies
  staleness on the entry it grabbed and restores it untouched if it turned out live — so a freshly
  recreated lock is never clobbered.
- **Invalidation** (`_sql_source_version` + `_read_valid_metadata`): a sidecar `*.meta.json` stores the
  `source_version`, source key, parquet size/checksum, row count, column count, and schema hash. SQL
  entries use `MAX(ORA_ROWSCN)|ROWS`. If the source version is `None`, the entry is validated by parquet
  bytes/schema. Warm reads check size + parquet footer (row count, column count, schema hash); the
  full-file SHA-256 is recomputed on every read only when `verify_checksum=True` (off by default, as
  it is an O(file size) read on the warm path).
- **Stable cold load** (in `_run_cached_parquet`): reads source version before and after running the
  write function; if it changed mid-stream, it discards the part file and retries, so a cache entry is
  never written for a torn read.
- **Atomic write** (`_write_parquet_file_entry`): the wrapped function streams into a unique `*.part`
  file (`pid + uuid` suffix); the entry is committed by `os.replace` of the parquet then the metadata
  sidecar (metadata is authoritative). Partials are cleaned up on failure.

### Database layer: `nfscache/database/`

- `oracle_streaming.py` — the canonical `@nfscache.sql_parquet` CLI: fetches with a cursor and writes
  batches via `pyarrow.parquet.ParquetWriter`, mapping Oracle column types to Arrow types
  (`_arrow_type` / `_rows_to_table`). The Oracle→Arrow helpers here are reused by `oracle_read.py` and
  `swarm_stream.py` via import; `oracle_read.py` keeps its own copy to stay free of `NFSCache`.
- `oracle_write.py` — reads a Parquet file with PyArrow and writes it to Oracle: maps Arrow types to
  Oracle DDL (`oracle_type`), validates identifiers (`oracle_identifier`), creates/drops the table,
  and `executemany` in batches; prints `current_scn` before/after.
- `oracle_read.py` — standalone Oracle smoke test: reads SQL into a PyArrow `Table` and logs it. No
  cache, no `NFSCache` dependency (only `oracle_env.apply_dotenv`).
- `oracle_pool.py` — process-local `oracledb` connection pool exposed as a `connect_factory`.
- `oracle_env.py` — a tiny dependency-free `.env` reader (no python-dotenv); degrades to defaults when
  `.env` is missing **or unreadable** rather than crashing.

### Util layer: `nfscache/util/`

- `generate_parquets.py` — builds test parquet files directly as PyArrow tables (numpy-backed
  columns) and writes them with `pyarrow.parquet`, via a `*.part` + `os.replace` promotion.
- `swarm_stream.py` — creates a test Oracle table, runs `@nfscache.sql_parquet` clients in separate
  processes, and rewrites the table between read waves to verify SQL cache invalidation under load.
  It inlines its own Oracle table setup/insert helpers (it is self-contained).

### Caveats / WIP

- The README "Production Notes" list the known gaps (structured metrics, failure tests, and a
  mixed multi-host NFS + SMB client pool). Validating `mkdir`/stale-lock recovery/`os.replace`
  on a real SMB mount is **done**: a macOS `mount_smbfs` client against a Windows 10 Pro share
  ran the streaming swarm (consistent cache: matching checksums, valid parquet, intact metadata)
  and the full locking suite — including stale reader/writer recovery — over the mount.
- `MAX(ORA_ROWSCN)` is block-level granularity unless the table was created `ROWDEPENDENCIES`; it is
  monotonic enough as a version token, and the row count in the version string is the extra guard.
