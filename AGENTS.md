# Repository Guidelines

## Project Structure & Module Organization

This is a Python 3.13 prototype for an NFS-backed Parquet cache around `DataContainer` objects. Core cache logic lives in `disk_cache/db_cache.py`, including authoritative metadata, read/write locking, heartbeats, and stale-lock recovery. Data wrapper types are in `disk_cache/data/`, and parquet generation utilities are in `disk_cache/util/`. Oracle demo and load/read CLIs live in `database/`. Top-level entry points are `main.py` for the local cache demo and `swarm.py` for process-level concurrency checks. Docker/Oracle bootstrap files are `Dockerfile`, `build_and_run.sh`, and `init/001_create_user_and_privs.sql`. Generated runtime data belongs in `parquet/` and `__cache__/`.

## Build, Test, and Development Commands

- `uv sync`: install dependencies from `pyproject.toml` and `uv.lock`.
- `uv run --no-cache --no-sync python -m main`: clear `__cache__`, then run cold/warm/reload demo.
- `uv run --no-cache --no-sync python -m swarm`: run the default process-level concurrency exercise.
- `uv run --no-cache --no-sync python -m unittest discover -s tests`: run the focused unit tests.
- `uv run --no-cache --no-sync python -m compileall -q disk_cache database tests main.py swarm.py`: syntax-check modules.
- `uv run --no-cache --no-sync python -m disk_cache.util.generate_parquets --seed 123`: generate reproducible parquet source data.
- `./build_and_run.sh [--wipe]`: build and start the local Oracle container on port `1521`.
- `uv run --no-cache --no-sync python -m database.oracle_write_container`: populate Oracle with generated data.
- `uv run --no-cache --no-sync python -m database.oracle_read "<SQL>"`: read Oracle data through the SQL cache.

When running `uv` inside restricted agent sandboxes, prefer `uv run --no-cache --no-sync ...` if normal cache access fails.

## Coding Style & Naming Conventions

Use standard Python style with 4-space indentation, descriptive snake_case names, and small functions that keep cache, data model, and Oracle concerns separated. Preserve the decorator API (`@dbcache.data_container_cache`, `@nfscache.sql`) and the `DataContainer.data.rows_data_pl` contract. Keep lock changes conservative: reader tokens and writer intent are directory locks with `lock.json` heartbeat metadata. No formatter or linter is configured; keep imports tidy and avoid unrelated rewrites.

## Testing Guidelines

Focused `unittest` coverage lives in `tests/`, including metadata integrity, corrupted cache recovery, normalized SQL metadata, overlapping warm readers, writer preference, heartbeat freshness, and stale-lock recovery. Treat `main.py` as the local smoke test and `swarm.py` as the process-level check for invalidation and concurrent clients. For Oracle changes, run the Docker bootstrap plus the relevant `database.oracle_*` CLI. Name future tests by behavior, for example `test_cache_invalidates_on_source_hash_change`.

## Commit & Pull Request Guidelines

Recent commits are short and scope-focused, often naming the touched feature, such as `normalized_sql`, `oracle_read`, or `@nfscache.sql`. Keep commits similarly concise. Pull requests should describe the cache behavior changed, list commands run, mention Oracle/NFS assumptions, and call out generated artifacts or local-only files that should not be committed.

## Security & Configuration Tips

Oracle defaults are development-only (`SOMEUSER`/`cache`, local `FREEPDB1`). Keep `.env`, generated parquet data, cache directories, and logs out of commits. Validate `mkdir` lock tokens, stale-lock recovery, and `os.replace` on the target NFS mount before treating this as production-safe.
