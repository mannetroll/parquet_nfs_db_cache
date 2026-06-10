# Repository Guidelines

## Project Structure & Module Organization

This is a Python 3.13 prototype for a shared-filesystem (NFS / SMB) Parquet cache. It reads and writes Parquet directly with PyArrow. Core cache logic lives in `nfscache/nfs_cache.py`, including authoritative metadata, read/write locking, heartbeats, and stale-lock recovery. Parquet generation and the concurrency swarm live in `nfscache/util/`. Oracle load/read/stream CLIs and the connection pool live in `nfscache/database/`. The main entry points are `nfscache.util.swarm_stream` for Oracle SQL streaming-cache concurrency checks and `nfscache.util.generate_parquets` for test data. Docker/Oracle bootstrap files are `Dockerfile`, `build_and_run.sh`, and `init/001_create_user_and_privs.sql`. Generated runtime data belongs in `parquet/` and `__cache__/`.

## Build, Test, and Development Commands

- `uv sync`: install dependencies from `pyproject.toml` and `uv.lock`.
- `uv run --no-cache --no-sync python -m nfscache.util.swarm_stream`: run Oracle SQL streaming-cache readers while a writer process rewrites the source table.
- `uv run --no-cache --no-sync python -m unittest discover -s tests`: run the focused unit tests.
- `uv run --no-cache --no-sync python -m compileall -q nfscache tests`: syntax-check modules.
- `uv run --no-cache --no-sync python -m nfscache.util.generate_parquets --seed 123`: generate reproducible parquet source data.
- `./build_and_run.sh [--wipe]`: build and start the local Oracle container on port `1521`.
- `uv run --no-cache --no-sync python -m nfscache.database.oracle_write parquet/A_TEST_1048576.parquet`: load a parquet file into Oracle.
- `uv run --no-cache --no-sync python -m nfscache.database.oracle_streaming "<SQL>" out.parquet`: stream Oracle SQL through the Parquet cache.
- `uv run --no-cache --no-sync python -m nfscache.database.oracle_read "<SQL>"`: standalone Oracle read into a PyArrow table (no cache).

When running `uv` inside restricted agent sandboxes, prefer `uv run --no-cache --no-sync ...` if normal cache access fails.

## Coding Style & Naming Conventions

Use standard Python style with 4-space indentation, descriptive snake_case names, and small functions that keep cache, data-generation, and Oracle concerns separated. Preserve the decorator API (`@nfscache.sql_parquet`) and its contract: the wrapped function writes a Parquet file to the supplied `parquet_path`/`output_path`, and the cache reads/writes Parquet with PyArrow only. Keep lock changes conservative: reader tokens and writer intent are directory locks with `lock.json` heartbeat metadata; staleness is judged in the file server's clock domain (a measured client/server clock offset, plus same-host PID liveness), and stale locks are stolen by atomic rename, not blind removal. No formatter or linter is configured; keep imports tidy and avoid unrelated rewrites.

## Testing Guidelines

Focused `unittest` coverage lives in `tests/`, including metadata integrity, corrupted cache recovery, normalized SQL metadata, SQL version invalidation, overlapping warm readers, writer preference, heartbeat freshness, and stale-lock recovery (atomic rename stealing, clock-skew-safe staleness, same-host dead-owner reclaim, and acquisition timeout), plus the Oracle pool/streaming helpers. Treat `swarm_stream.py` as the Oracle SQL process-level check for invalidation and concurrent clients (note: its `ProcessPoolExecutor` cannot run inside restricted sandboxes). For Oracle changes, run the Docker bootstrap plus the relevant `nfscache.database.oracle_*` CLI. Name future tests by behavior, for example `test_cache_invalidates_on_source_scn_change`.

## Commit & Pull Request Guidelines

Recent commits are short and scope-focused, often naming the touched feature, such as `normalized_sql`, `oracle_read`, or `sql_query`. Keep commits similarly concise. Pull requests should describe the cache behavior changed, list commands run, mention Oracle/NFS/SMB assumptions, and call out generated artifacts or local-only files that should not be committed.

## Security & Configuration Tips

Oracle defaults are development-only (`SOMEUSER`/`cache`, local `FREEPDB1`). Keep `.env`, generated parquet data, cache directories, and logs out of commits. Validate `mkdir` lock tokens, stale-lock recovery, and `os.replace` on the target NFS or SMB mount before treating this as production-safe. These have been validated against a real SMB share (a macOS `mount_smbfs` client against a Windows 10 Pro share) — the streaming swarm and the full locking suite, including stale reader/writer recovery, pass over the mount; a mixed multi-host NFS + SMB client pool is still unvalidated.
