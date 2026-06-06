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
uv run -m main          # demo: cold load -> warm hit -> source change -> reload -> warm hit
uv run -m swarm         # multi-process concurrency test (4 clients, 12 get waves, 6 regenerations)
uv run python -m nfs_cache.util.generate_parquets [--seed N]   # (re)generate test parquet files
```

`swarm.py` takes flags to size the run (`--clients`, `--generators`, `--gets-per-client`,
`--generations`, `--n-rows`, `--cols`, `--data-dir`, `--cache-dir`); see README for a small example.

There is no test suite, linter, or formatter configured. `main.py` and `swarm.py` are the de facto
behavior checks.

### Oracle (optional, for the database source path)

```bash
./build_and_run.sh [--wipe]   # build + run gvenzl/oracle-free in Docker on :1521 (service FREEPDB1)
```

`init/001_create_user_and_privs.sql` runs once on first DB init, creating user `SOMEUSER`/`cache`
and granting `SELECT ON V_$DATABASE` (needed for `current_scn`). Oracle connection settings are read
from CLI flags, then overridden by a `.env` file (see `database/oracle_env.py`, keys `ORACLE_HOST`,
`ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_TABLE`, `ORACLE_BATCH_SIZE`).

```bash
uv run -m database.oracle_write_container          # generate a DataContainer and load it into Oracle
uv run -m database.oracle_write <parquet_path>     # load an existing parquet into Oracle
uv run -m database.oracle_read "<SQL>"             # read SQL into a DataContainer (standalone main)
```

## Architecture

### Core: `nfs_cache/db_cache.py` — `DBCache`

The whole caching engine is one class exposing a decorator, `@dbcache.data_container_cache`, that
wraps any `Callable[..., DataContainer]`. The wrapped function becomes the cold-load source; the
decorator handles locking, invalidation, read, and write. Key mechanics to understand before changing:

- **Cache key / path** (`_display_key` + `_cache_path`): if the first positional arg (or `path` kwarg)
  is a path-like, the file path *is* the key and the cache mirrors that path under `cache_dir`.
  Absolute paths and paths containing `..` are hashed into `_absolute/` or `_relative/` subdirs.
  Non-path args hash `(module, qualname, args, kwargs)` into a parquet filename.
- **Locking** (`_acquire_lock`): a per-cache-key directory lock via `mkdir` (atomic on NFS),
  busy-waited with `poll_seconds`. Released with `rmdir` in a `finally`.
- **Invalidation** (`_source_version` + `_is_current`): a sidecar `*.meta.json` stores a
  `source_version` string. Default version for a file source is `sha256:<hash>` of file content;
  for DB sources pass a custom `source_version=` callable (intended use: an Oracle SCN). If the
  callable returns `None`, versioning is disabled and the cache is always treated as current.
- **Stable cold load** (`_load_stable`): reads source version before and after calling the source
  function; if it changed mid-load, it retries, so a cache entry is never written for a torn read.
- **Atomic write** (`_write_data_container`): writes to a unique `*.part` file
  (`pid + uuid` suffix), then `os.replace` onto the final path. Partials are cleaned up on failure.
  Only `DataContainer.data.rows_data_pl` (the Polars DataFrame) is persisted, via `pyarrow.parquet`.

### Data model: `nfs_cache/data/`

- `DataContainer` (`data_container.py`): `__slots__`-based wrapper built from
  `{"headers": tuple, "data": pl.DataFrame}`. Holds a single `DataHolder` on `.data`.
- `DataHolder` (`data_holder.py`): `.headers` (tuple) and `.rows_data_pl` (the Polars DataFrame).
  This `data.rows_data_pl` path is what the cache reads/writes — keep it a `pl.DataFrame`.

### Database layer: `database/`

Each `oracle_*.py` is a self-contained CLI. `oracle_write*.py` map Polars dtypes to Oracle DDL
(`oracle_type`), validate identifiers (`oracle_identifier`), create/drop the table, and `executemany`
in batches; they print `current_scn` before/after to demonstrate the SCN-based version token.
`oracle_env.py` is a tiny dependency-free `.env` reader (no python-dotenv).

### Caveats / WIP

- `database/oracle_read.py` decorates with `@dbcache.sql_container_cache`, but `DBCache` currently
  only defines `data_container_cache`. This is unfinished — that method does not exist yet, so
  `oracle_read.py` will fail at import/decoration time until it's added or the decorator is renamed.
- The README "Production Notes" list the known gaps (stale-lock recovery for crashed writers,
  validating `mkdir`/`os.replace` on a real NFS mount, structured metrics, failure tests).
