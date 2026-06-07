# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A prototype NFS-backed cache for `DataContainer` objects whose payload is a Polars
`DataFrame`. Cold loads read from any slow source (Oracle, a local parquet file, etc.);
warm loads read a Parquet cache file with `polars.read_parquet`. The design target is an
Oracle-on-NFS deployment with many concurrent clients, so most of the complexity is about
making concurrent writes and cache invalidation safe over a shared filesystem.

## Commands

This project uses `uv` (Python 3.13). Dependencies live in `pyproject.toml` / `uv.lock`.

```bash
uv sync
uv run --no-cache --no-sync python -m nfscache.util.main
uv run --no-cache --no-sync python -m nfscache.util.swarm_file
uv run --no-cache --no-sync python -m nfscache.util.swarm_sql
uv run --no-cache --no-sync python -m unittest discover -s tests
uv run --no-cache --no-sync python -m compileall -q nfscache tests
uv run --no-cache --no-sync python -m nfscache.util.generate_parquets [--seed N]
```

`main.py` demonstrates cold load, warm hit, source regeneration, reload, and warm hit.
`swarm_file.py` takes flags to size the run (`--clients`, `--generators`, `--gets-per-client`,
`--generations`, `--n-rows`, `--cols`, `--data-dir`, `--cache-dir`); see README for a small example.
`swarm_sql.py` takes similar sizing flags (`--clients`, `--writers`, `--gets-per-client`,
`--generations`, `--n-rows`, `--batch-size`, `--table`, `--cache-dir`) and requires Oracle.

The project has focused `unittest` coverage under `tests/` for metadata integrity and locking.
`main.py`, `swarm_file.py`, `swarm_sql.py`, and the `nfscache.database.oracle_read` cold/warm logging are still
useful behavior checks. There is no linter or formatter configured.

> **Running uv inside Claude Code's command sandbox:** plain `uv run` fails with
> `Failed to initialize cache ... .git: Operation not permitted` because the sandbox blocks `.git`
> files (uv's package cache contains some). Use `uv run --no-cache --no-sync ...` (the env is already
> synced in `.venv`). This is a sandbox workaround, not a `UV_CACHE_DIR` override. Do **not** set
> `UV_CACHE_DIR`. Connecting to local Oracle on `localhost:1521` works from the sandbox.

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
uv run --no-cache --no-sync python -m nfscache.database.oracle_write_container
uv run --no-cache --no-sync python -m nfscache.database.oracle_write parquet/A_TEST_1048576.parquet
uv run --no-cache --no-sync python -m nfscache.database.oracle_read "select * from A_TEST_1048576"
uv run --no-cache --no-sync python -m nfscache.util.swarm_sql
```

`oracle_read` goes *through the cache*: a miss logs `Serving from Oracle (cache miss): ...`, a hit
logs `Returning cached object: sql/<TABLE>/<hash>.parquet version=<TABLE>@SCN:<scn>|ROWS:<n>`. To see a
full cold -> warm -> reload cycle, run it twice, then `oracle_write_container` (advances the SCN), then
run it again. `oracle_read` currently takes its cached connection settings from `oracle_args()`
(defaults plus `.env`), so `.env` is the authoritative connection override.

## Architecture

### Core: `nfscache/nfs_cache.py` — `NFSCache`

The whole caching engine is one class exposing **two decorators** that wrap any
`Callable[..., DataContainer]`. The wrapped function is the cold-load source; the decorator handles
locking, invalidation, read, and write. Both funnel into the shared `_run_cached(display_key,
version_fn, load_fn)` flow — they differ only in how the cache key and source version are derived:

- `@nfscache.parquet` — for file/in-process sources. Key and version come from
  the decorated call's `filename` argument.
- `@nfscache.sql` — for SQL sources. First arg is the SQL string (optional
  `return_cols=` kwarg). Key is `sql/<TABLE>/<sha256(normalized_sql|cols)>.parquet`; version is read
  from Oracle as `<TABLE>@SCN:<MAX(ORA_ROWSCN)>|ROWS:<count>` via `_sql_source_version`, using
  `NFSCache.connect_factory`. The table is parsed from the SQL with `_FROM_RE`.

`connect_factory` is an opaque `Callable[[], connection]` set on the `NFSCache` instance (see
`nfscache/database/oracle_read.py`, which assigns `nfscache.connect_factory = lambda: connect(oracle_args())`).
It is kept generic so `nfs_cache.py` never imports `oracledb`. If unset, SQL versioning is disabled.

Key mechanics to understand before changing:

- **Cache key / path** (`_parquet_display_key` + `_cache_path`): for `parquet`,
  the `filename` argument is the key and the cache mirrors that path under
  `cache_dir`. Absolute paths and paths containing `..` are hashed into
  `_absolute/` or `_relative/` subdirs.
- **Locking** (`_acquire_read_lock` + `_acquire_write_lock`): a per-cache-key directory read/write
  lock via `mkdir` (atomic on NFS). Warm readers create per-reader tokens and can overlap. Writers
  create a writer-intent directory, block new readers, and wait for active readers to drain before
  writing or revalidating stale entries. Reader tokens and writer intent store `lock.json` metadata
  (`hostname`, `pid`, `uuid`, `created_at`, `last_seen`) and update `last_seen` by heartbeat while
  held. Locks older than `stale_lock_seconds` are broken. The default stale timeout is 30 minutes so
  a live 10-minute Oracle cold read is not treated as abandoned.
- **Invalidation** (`_source_version` + `_read_valid_metadata`): a sidecar `*.meta.json` stores the
  `source_version`, source key, parquet size/checksum, row count, column count, and schema hash.
  Default version for a file source is `sha256:<hash>` of file content; SQL entries use
  `MAX(ORA_ROWSCN)|ROWS`. If the source version is `None`, metadata still exists and the entry is
  validated by parquet bytes/schema.
- **Stable cold load** (`_load_stable`): reads source version before and after calling the source
  function; if it changed mid-load, it retries, so a cache entry is never written for a torn read.
- **Atomic write** (`_write_data_container`): writes to a unique `*.part` file
  (`pid + uuid` suffix), then `os.replace` onto the final path. Partials are cleaned up on failure.
  Only `DataContainer.data.rows_data_pl` (the Polars DataFrame) is persisted, via `pyarrow.parquet`.

### Data model: `nfscache/data/`

- `DataContainer` (`data_container.py`): `__slots__`-based wrapper built from
  `{"headers": tuple, "data": pl.DataFrame}`. Holds a single `DataHolder` on `.data`.
- `DataHolder` (`data_holder.py`): `.headers` (tuple) and `.rows_data_pl` (the Polars DataFrame).
  This `data.rows_data_pl` path is what the cache reads/writes — keep it a `pl.DataFrame`.

### Database layer: `nfscache/database/`

Each `oracle_*.py` is a self-contained CLI. `oracle_write*.py` map Polars dtypes to Oracle DDL
(`oracle_type`), validate identifiers (`oracle_identifier`), create/drop the table, and `executemany`
in batches; they print `current_scn` before/after to demonstrate the SCN-based version token.
`oracle_read.py` wires the SQL cache to Oracle (sets `connect_factory`, decorates with
`nfscache.sql`). `oracle_env.py` is a tiny dependency-free `.env` reader (no python-dotenv);
it degrades to defaults when `.env` is missing **or unreadable** rather than crashing.
`swarm_sql.py` creates a test table, runs `@nfscache.sql` clients in separate processes, and rewrites
the table between read waves to verify SQL cache invalidation under load.

### Caveats / WIP

- The README "Production Notes" list the known gaps (validating `mkdir`/stale-lock recovery/
  `os.replace` on a real NFS mount, structured metrics, failure tests).
- `MAX(ORA_ROWSCN)` is block-level granularity unless the table was created `ROWDEPENDENCIES`; it is
  monotonic enough as a version token, and the row count in the version string is the extra guard.
