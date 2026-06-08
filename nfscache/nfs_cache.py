import errno
import functools
import hashlib
import inspect
import json
import os
import re
import shutil
import socket
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from nfscache.data.data_container import DataContainer

CACHE_METADATA_VERSION = 1
CACHE_WRITER_VERSION = "nfscache.v1"
LOCK_METADATA_VERSION = 1
SQL_CACHE_FINGERPRINT_HEX_LENGTH = 16


class _LockLease:
    def __init__(
        self,
        path: Path,
        stop_event: threading.Event,
        thread: threading.Thread,
    ) -> None:
        self.path = path
        self.stop_event = stop_event
        self.thread = thread


class NFSCache:
    # Cache-key version stamp for a table source. ORA_ROWSCN advances on any
    # change to a row's block, so MAX(ORA_ROWSCN) is a cheap version token; the
    # row count guards against table swaps that do not advance the SCN.
    VERSION_SQL = (
        "SELECT COUNT(*) AS N_ROWS, MAX(ORA_ROWSCN) AS MAX_ROWSCN FROM {table}"
    )
    _FROM_RE = re.compile(r"\bfrom\s+([A-Za-z0-9_$#.\"]+)", re.IGNORECASE)

    def __init__(
        self,
        cache_dir: Path,
        *,
        poll_seconds: float = 0.1,
        stale_lock_seconds: float = 1800.0,
        heartbeat_seconds: float | None = None,
        source_version: Callable[..., object] | None = None,
        connect_factory: Callable[[], object] | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.poll_seconds = poll_seconds
        if stale_lock_seconds <= 0:
            raise ValueError("stale_lock_seconds must be positive")
        self.stale_lock_seconds = stale_lock_seconds
        if heartbeat_seconds is None:
            heartbeat_seconds = max(poll_seconds, min(5.0, stale_lock_seconds / 3))
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        self.heartbeat_seconds = heartbeat_seconds
        self.source_version = source_version
        # Opaque callable returning a DB connection (anything with a
        # context-manager `cursor()`); used by `sql` to read the
        # source version. Kept generic so the cache does not depend on oracledb.
        self.connect_factory = connect_factory

    def parquet[**P](
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        signature = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            filename = self._filename_arg(signature, args_tuple, kwargs_dict)
            display_key = self._parquet_display_key(filename)
            return self._run_cached(
                display_key,
                lambda: self._source_version(filename, args_tuple, kwargs_dict),
                lambda: func(*args, **kwargs),
                source_sql=None,
            )

        return wrapper

    def sql[**P](
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        signature = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            sql = self._sql_arg(signature, args_tuple, kwargs_dict)
            normalized_sql = self._normalize_sql(str(sql))
            return_cols = kwargs_dict.get("return_cols")
            display_key = self._sql_display_key(normalized_sql, return_cols)
            return self._run_cached(
                display_key,
                lambda: self._sql_source_version(normalized_sql),
                lambda: func(*args, **kwargs),
                source_sql=normalized_sql,
            )

        return wrapper

    def sql_parquet[**P](
        self,
        func: Callable[P, object],
    ) -> Callable[P, Path]:
        signature = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Path:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            sql = self._sql_arg(signature, args_tuple, kwargs_dict)
            output_path = Path(
                self._path_arg(signature, args_tuple, kwargs_dict)
            )
            normalized_sql = self._normalize_sql(str(sql))
            return_cols = kwargs_dict.get("return_cols")
            display_key = self._sql_display_key(normalized_sql, return_cols)

            def write_part(part_path: Path) -> None:
                part_args, part_kwargs = self._replace_path_arg(
                    signature,
                    args_tuple,
                    kwargs_dict,
                    part_path,
                )
                func(*part_args, **part_kwargs)

            cache_path = self._run_cached_parquet(
                display_key,
                lambda: self._sql_source_version(normalized_sql),
                write_part,
                source_sql=normalized_sql,
            )
            self._copy_cached_parquet(cache_path, output_path)
            return output_path

        return wrapper

    def _run_cached(
        self,
        display_key: str,
        version_fn: Callable[[], str | None],
        load_fn: Callable[[], DataContainer],
        *,
        source_sql: str | None,
    ) -> DataContainer:
        cache_path = self._cache_path(display_key)
        meta_path = cache_path.with_name(f"{cache_path.name}.meta.json")
        lock_path = cache_path.with_name(f"{cache_path.name}.lock")

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        source_version = version_fn()
        reader_lease = self._acquire_read_lock(lock_path)
        try:
            cached = self._read_if_cached(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached is not None:
                return cached
        finally:
            self._release_read_lock(reader_lease)

        writer_lease = self._acquire_write_lock(lock_path)
        try:
            source_version = version_fn()
            cached = self._read_if_cached(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached is not None:
                return cached

            part_path = self._part_path(cache_path)
            meta_part_path = self._part_path(meta_path)
            try:
                data, source_version = self._load_stable(version_fn, load_fn)
                self._write_data_container(
                    part_path,
                    cache_path,
                    meta_part_path,
                    meta_path,
                    display_key,
                    source_version,
                    source_sql,
                    data,
                )
                return data
            except Exception:
                self._remove_file(part_path)
                self._remove_file(meta_part_path)
                raise
        finally:
            self._release_write_lock(writer_lease)

    def _run_cached_parquet(
        self,
        display_key: str,
        version_fn: Callable[[], str | None],
        write_fn: Callable[[Path], None],
        *,
        source_sql: str | None,
    ) -> Path:
        cache_path = self._cache_path(display_key)
        meta_path = cache_path.with_name(f"{cache_path.name}.meta.json")
        lock_path = cache_path.with_name(f"{cache_path.name}.lock")

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        source_version = version_fn()
        reader_lease = self._acquire_read_lock(lock_path)
        try:
            cached_path = self._cached_parquet_path_if_valid(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached_path is not None:
                return cached_path
        finally:
            self._release_read_lock(reader_lease)

        writer_lease = self._acquire_write_lock(lock_path)
        try:
            source_version = version_fn()
            cached_path = self._cached_parquet_path_if_valid(
                cache_path,
                meta_path,
                display_key,
                source_version,
                source_sql,
            )
            if cached_path is not None:
                return cached_path

            while True:
                part_path = self._part_path(cache_path)
                meta_part_path = self._part_path(meta_path)
                try:
                    source_version_before = version_fn()
                    write_fn(part_path)
                    source_version_after = version_fn()
                    if source_version_before == source_version_after:
                        self._write_parquet_file_entry(
                            part_path,
                            cache_path,
                            meta_part_path,
                            meta_path,
                            display_key,
                            source_version_after,
                            source_sql,
                        )
                        return cache_path

                    print(
                        "Source changed while streaming; retrying cold load...",
                        flush=True,
                    )
                    self._remove_file(part_path)
                    self._remove_file(meta_part_path)
                except Exception:
                    self._remove_file(part_path)
                    self._remove_file(meta_part_path)
                    raise
        finally:
            self._release_write_lock(writer_lease)

    def _acquire_read_lock(self, lock_path: Path) -> _LockLease:
        readers_path = lock_path / "readers"
        writer_path = lock_path / "writer"
        # Stable token name for this whole acquisition. Generating a fresh UUID
        # on every retry mints a new directory each spin, which leaks reader
        # folders on shares where removal is racy (e.g. Windows/SMB).
        reader_path = readers_path / self._reader_lock_name()

        while True:
            if not self._ensure_lock_dirs(lock_path):
                time.sleep(self.poll_seconds)
                continue
            if writer_path.exists():
                self._break_stale_lock(writer_path)
                time.sleep(self.poll_seconds)
                continue

            try:
                reader_path.mkdir()
            except FileExistsError:
                # Our own token survived a previous iteration (a release that
                # lost the rmdir race). Clear it and retry rather than spin.
                self._remove_lock_dir(reader_path)
                time.sleep(self.poll_seconds)
                continue
            except FileNotFoundError:
                time.sleep(self.poll_seconds)
                continue
            except OSError as exc:
                if self._is_transient_lock_mkdir_error(exc):
                    time.sleep(self.poll_seconds)
                    continue
                raise

            try:
                reader_lease = self._start_lock_heartbeat(
                    reader_path,
                    "reader",
                    create_missing=True,
                )
            except OSError as exc:
                self._remove_lock_dir(reader_path)
                if not self._is_transient_lock_mkdir_error(exc):
                    raise
                time.sleep(self.poll_seconds)
                continue

            if not writer_path.exists():
                return reader_lease

            self._release_read_lock(reader_lease)
            time.sleep(self.poll_seconds)

    def _acquire_write_lock(self, lock_path: Path) -> _LockLease:
        readers_path = lock_path / "readers"
        writer_path = lock_path / "writer"

        while True:
            if not self._ensure_lock_dirs(lock_path):
                time.sleep(self.poll_seconds)
                continue
            try:
                writer_path.mkdir()
                writer_lease = self._start_lock_heartbeat(writer_path, "writer")
                break
            except FileExistsError:
                self._break_stale_lock(writer_path)
                time.sleep(self.poll_seconds)
            except FileNotFoundError:
                time.sleep(self.poll_seconds)
                continue
            except OSError as exc:
                if self._is_transient_lock_mkdir_error(exc):
                    time.sleep(self.poll_seconds)
                    continue
                raise

        try:
            while self._has_readers(readers_path):
                time.sleep(self.poll_seconds)
            return writer_lease
        except Exception:
            self._release_write_lock(writer_lease)
            raise

    def _release_read_lock(self, lease: _LockLease | Path) -> None:
        reader_path = self._lease_path(lease)
        self._stop_lock_heartbeat(lease)
        lock_path = reader_path.parent.parent
        self._remove_lock_dir_resilient(reader_path)
        self._cleanup_lock_dirs(lock_path)

    def _release_write_lock(self, lease: _LockLease | Path) -> None:
        writer_path = self._lease_path(lease)
        self._stop_lock_heartbeat(lease)
        lock_path = writer_path.parent
        self._remove_lock_dir_resilient(writer_path)
        self._cleanup_lock_dirs(lock_path)

    def _remove_lock_dir_resilient(self, lock_path: Path) -> None:
        # A heartbeat thread that was mid-write when we asked it to stop can
        # re-create lock.json after _remove_lock_dir deletes it, leaving a
        # non-empty dir that rmdir then skips. Retry until the dir is gone so a
        # stopped lock never lingers and blocks other clients (Windows/SMB).
        for _ in range(5):
            self._remove_lock_dir(lock_path)
            if not lock_path.exists():
                return
            time.sleep(self.poll_seconds)
        self._remove_lock_dir(lock_path)

    @staticmethod
    def _lease_path(lease: _LockLease | Path) -> Path:
        return lease.path if isinstance(lease, _LockLease) else lease

    @staticmethod
    def _stop_lock_heartbeat(lease: _LockLease | Path) -> None:
        if not isinstance(lease, _LockLease):
            return

        lease.stop_event.set()
        # Join long enough to cover a heartbeat blocked on a slow share write,
        # so the thread cannot re-create lock.json after we remove the dir.
        lease.thread.join(timeout=5.0)

    def _ensure_lock_dirs(self, lock_path: Path) -> bool:
        # Returns True only when readers/ is confirmed to exist. A transient
        # mkdir failure (common on NFS/SMB) returns False so the caller retries
        # rather than racing into a child mkdir that fails with FileNotFoundError.
        readers_path = lock_path / "readers"
        try:
            readers_path.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            return True
        except OSError as exc:
            if not self._is_transient_lock_mkdir_error(exc):
                raise
            return readers_path.exists()
        return True

    def _has_readers(self, readers_path: Path) -> bool:
        has_reader = False
        try:
            reader_paths = list(readers_path.iterdir())
        except FileNotFoundError:
            return False

        for reader_path in reader_paths:
            if not reader_path.is_dir():
                self._remove_file(reader_path)
                continue
            if self._break_stale_lock(reader_path):
                continue
            has_reader = True

        return has_reader

    @staticmethod
    def _cleanup_lock_dirs(lock_path: Path) -> None:
        # Keep shared coordination directories in place. Removing them after a
        # release races with other clients creating reader tokens on NFS.
        _ = lock_path

    @staticmethod
    def _is_transient_lock_mkdir_error(exc: OSError) -> bool:
        return exc.errno in {errno.ENOENT, errno.EINVAL}

    @staticmethod
    def _reader_lock_name() -> str:
        hostname = socket.gethostname().replace("/", "_")[:16]
        return f"r.{hostname}.{os.getpid()}.{uuid.uuid4().hex[:16]}"

    def _start_lock_heartbeat(
        self,
        lock_path: Path,
        lock_type: str,
        *,
        create_missing: bool = False,
    ) -> _LockLease:
        lock_uuid = uuid.uuid4().hex
        created_at = self._utc_now()
        self._write_lock_metadata(
            lock_path,
            lock_type,
            lock_uuid,
            created_at,
            create_missing=create_missing,
        )
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._heartbeat_lock,
            args=(lock_path, lock_type, lock_uuid, created_at, stop_event),
            daemon=True,
        )
        thread.start()
        return _LockLease(lock_path, stop_event, thread)

    def _heartbeat_lock(
        self,
        lock_path: Path,
        lock_type: str,
        lock_uuid: str,
        created_at: str,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.heartbeat_seconds):
            if not lock_path.exists():
                return
            try:
                self._write_lock_metadata(lock_path, lock_type, lock_uuid, created_at)
            except OSError:
                return

    def _write_lock_metadata(
        self,
        lock_path: Path,
        lock_type: str,
        lock_uuid: str,
        created_at: str,
        *,
        create_missing: bool = False,
    ) -> None:
        if create_missing:
            lock_path.mkdir(parents=True, exist_ok=True)

        metadata_path = lock_path / "lock.json"
        part_path = metadata_path.with_name(
            f"l.{os.getpid()}.{uuid.uuid4().hex[:16]}.tmp"
        )
        metadata = {
            "metadata_version": LOCK_METADATA_VERSION,
            "lock_type": lock_type,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "uuid": lock_uuid,
            "created_at": created_at,
            "last_seen": self._utc_now(),
        }
        part_path.write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(part_path, metadata_path)

    def _break_stale_lock(self, lock_path: Path) -> bool:
        if not lock_path.exists() or not self._is_stale_lock(lock_path):
            return False

        print(f"Breaking stale cache lock: {lock_path}", flush=True)
        self._remove_lock_dir(lock_path)
        return not lock_path.exists()

    def _is_stale_lock(self, lock_path: Path) -> bool:
        try:
            metadata_mtime = (lock_path / "lock.json").stat().st_mtime
        except FileNotFoundError:
            try:
                metadata_mtime = lock_path.stat().st_mtime
            except FileNotFoundError:
                return False

        return (time.time() - metadata_mtime) > self.stale_lock_seconds

    @staticmethod
    def _remove_lock_dir(lock_path: Path) -> None:
        try:
            children = list(lock_path.iterdir())
        except FileNotFoundError:
            return
        except NotADirectoryError:
            try:
                lock_path.unlink()
            except (FileNotFoundError, OSError):
                pass
            return

        for child in children:
            try:
                if child.is_dir() and not child.is_symlink():
                    child.rmdir()
                else:
                    child.unlink()
            except (FileNotFoundError, OSError):
                pass

        try:
            lock_path.rmdir()
        except (FileNotFoundError, OSError):
            pass

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(UTC).isoformat()

    def _read_if_cached(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> DataContainer | None:
        metadata = self._read_valid_metadata(
            cache_path,
            meta_path,
            display_key,
            source_version,
            source_sql,
        )
        if metadata is None:
            return None

        version_preview = self._version_preview(metadata.get("source_version"))
        print(
            f"Returning cached object: {display_key}{version_preview}...",
            flush=True,
        )
        try:
            return self._read_data_container(cache_path)
        except Exception as exc:
            print(
                f"Ignoring corrupt cache entry: {display_key}: "
                f"parquet read failed: {exc}",
                flush=True,
            )
            return None

    def _read_data_container(self, cache_path: Path) -> DataContainer:
        df = pl.read_parquet(cache_path)
        return DataContainer({"headers": tuple(df.columns), "data": df})

    def _cached_parquet_path_if_valid(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> Path | None:
        metadata = self._read_valid_metadata(
            cache_path,
            meta_path,
            display_key,
            source_version,
            source_sql,
        )
        if metadata is None:
            return None

        version_preview = self._version_preview(metadata.get("source_version"))
        print(
            f"Returning cached object: {display_key}{version_preview}...",
            flush=True,
        )
        return cache_path

    def _load_stable(
        self,
        version_fn: Callable[[], str | None],
        load_fn: Callable[[], DataContainer],
    ) -> tuple[DataContainer, str | None]:
        while True:
            source_version_before = version_fn()
            data = load_fn()
            source_version_after = version_fn()
            if source_version_before == source_version_after:
                return data, source_version_after

            print("Source changed while reading; retrying cold load...", flush=True)

    def _write_data_container(
        self,
        part_path: Path,
        cache_path: Path,
        meta_part_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
        data: DataContainer,
    ) -> None:
        df = data.data.rows_data_pl
        if not isinstance(df, pl.DataFrame):
            raise TypeError(
                "DataContainer.data.rows_data_pl must be a Polars DataFrame"
            )

        table = df.to_arrow()
        try:
            with pq.ParquetWriter(str(part_path), table.schema) as writer:
                writer.write_table(table)
            self._write_metadata(
                meta_part_path,
                display_key=display_key,
                cache_path=cache_path,
                parquet_path=part_path,
                source_version=source_version,
                source_sql=source_sql,
                row_count=table.num_rows,
                column_count=table.num_columns,
                schema=table.schema,
            )
            # Commit protocol: metadata is authoritative. A cache entry is not
            # complete until the final metadata sidecar exists and matches the
            # final parquet bytes. Crashes before this point leave an entry that
            # readers reject as incomplete or corrupt and reload.
            os.replace(part_path, cache_path)
            os.replace(meta_part_path, meta_path)
        except Exception:
            self._remove_file(part_path)
            self._remove_file(meta_part_path)
            raise

    def _write_parquet_file_entry(
        self,
        part_path: Path,
        cache_path: Path,
        meta_part_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> None:
        try:
            parquet_metadata = pq.read_metadata(str(part_path))
            schema = pq.read_schema(str(part_path))
            self._write_metadata(
                meta_part_path,
                display_key=display_key,
                cache_path=cache_path,
                parquet_path=part_path,
                source_version=source_version,
                source_sql=source_sql,
                row_count=parquet_metadata.num_rows,
                column_count=len(schema),
                schema=schema,
            )
            os.replace(part_path, cache_path)
            os.replace(meta_part_path, meta_path)
        except Exception:
            self._remove_file(part_path)
            self._remove_file(meta_part_path)
            raise

    def _copy_cached_parquet(self, cache_path: Path, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_part = self._part_path(output_path)
        try:
            shutil.copyfile(cache_path, output_part)
            os.replace(output_part, output_path)
        except Exception:
            self._remove_file(output_part)
            raise

    def _write_metadata(
        self,
        meta_part_path: Path,
        *,
        display_key: str,
        cache_path: Path,
        parquet_path: Path,
        source_version: str | None,
        source_sql: str | None,
        row_count: int,
        column_count: int,
        schema: object,
    ) -> None:
        schema_metadata = self._schema_metadata(schema)
        metadata = {
            "metadata_version": CACHE_METADATA_VERSION,
            "writer_version": CACHE_WRITER_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "source_key": display_key,
            "source_version": source_version,
            "parquet": {
                "file_name": cache_path.name,
                "size_bytes": parquet_path.stat().st_size,
                "sha256": self._file_hash(parquet_path),
            },
            "data": {
                "row_count": row_count,
                "column_count": column_count,
                "schema_hash": self._schema_hash(schema_metadata),
            },
        }
        if source_sql is not None:
            metadata["source_sql"] = source_sql
        meta_part_path.write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _source_version(
        self,
        filename: object,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> str | None:
        if self.source_version is not None:
            version = self.source_version(*args, **kwargs)
            return None if version is None else str(version)

        if isinstance(filename, (str, os.PathLike)):
            path = Path(filename)
            if path.is_file():
                return f"sha256:{self._file_hash(path)}"

        return None

    def _file_hash(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
                digest.update(chunk)

        return digest.hexdigest()

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        return " ".join(sql.split()).strip().rstrip(";")

    # Parameter names the SQL/path arguments may appear under. The decorated
    # function's signature is matched against these so call sites can name the
    # arguments naturally (e.g. `sql_query`, `parquet_path`).
    _SQL_ARG_NAMES = ("sql", "sql_query")
    _PATH_ARG_NAMES = ("parquet_path", "output_path")

    @staticmethod
    def _sql_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        for name in NFSCache._SQL_ARG_NAMES:
            if name in kwargs:
                return kwargs[name]

        try:
            bound = signature.bind_partial(*args, **kwargs)
        except TypeError:
            if args:
                return args[0]
            raise

        for name in NFSCache._SQL_ARG_NAMES:
            if name in bound.arguments:
                return bound.arguments[name]

        if args:
            return args[0]

        raise TypeError("sql requires a sql argument")

    @staticmethod
    def _path_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        bound = signature.bind_partial(*args, **kwargs)
        for name in NFSCache._PATH_ARG_NAMES:
            if name in bound.arguments:
                return bound.arguments[name]

        raise TypeError("sql_parquet requires an output path argument")

    @staticmethod
    def _replace_path_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        path: Path,
    ) -> tuple[tuple[object, ...], dict[str, object]]:
        bound = signature.bind_partial(*args, **kwargs)
        for name in NFSCache._PATH_ARG_NAMES:
            if name in bound.arguments:
                bound.arguments[name] = path
                return bound.args, bound.kwargs

        raise TypeError("sql_parquet requires an output path argument")

    def _table_from_sql(self, sql: str) -> str | None:
        match = self._FROM_RE.search(sql)
        if match is None:
            return None
        # Strip quoting; keep schema.table as-is for the version query.
        return match.group(1).strip('"')

    def _sql_display_key(
        self,
        sql: str,
        return_cols: object | None = None,
    ) -> str:
        normalized_sql = self._normalize_sql(sql)
        if return_cols:
            cols_key = "-".join(sorted(str(col).upper() for col in return_cols))
        else:
            cols_key = "-"
        table_name = (self._table_from_sql(normalized_sql) or "NO_TABLE").upper()
        fingerprint = hashlib.sha256(
            f"{normalized_sql}|{cols_key}".encode("utf-8")
        ).hexdigest()[:SQL_CACHE_FINGERPRINT_HEX_LENGTH]
        return f"sql/{table_name}/{fingerprint}.parquet"

    def _sql_source_version(self, sql: str) -> str | None:
        if self.connect_factory is None:
            return None

        table_name = self._table_from_sql(self._normalize_sql(sql))
        if table_name is None:
            return None

        version_query = self.VERSION_SQL.format(table=table_name)
        with self.connect_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(version_query)
                row = cursor.fetchone()

        n_rows = row[0] if row else 0
        scn = int(row[1]) if row and row[1] is not None else 0
        return f"{table_name.upper()}@SCN:{scn}|ROWS:{n_rows}"

    def _read_valid_metadata(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> dict[str, object] | None:
        if not self._is_complete(cache_path):
            return None

        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            print(
                f"Ignoring incomplete cache entry: {display_key}: "
                "missing metadata",
                flush=True,
            )
            return None
        except json.JSONDecodeError as exc:
            print(
                f"Ignoring corrupt cache entry: {display_key}: "
                f"metadata is not valid JSON: {exc}",
                flush=True,
            )
            return None

        reason = self._metadata_reject_reason(
            metadata,
            cache_path,
            display_key,
            source_version,
            source_sql,
        )
        if reason is not None:
            print(f"Ignoring cache entry: {display_key}: {reason}", flush=True)
            return None

        return metadata

    def _metadata_reject_reason(
        self,
        metadata: object,
        cache_path: Path,
        display_key: str,
        source_version: str | None,
        source_sql: str | None,
    ) -> str | None:
        if not isinstance(metadata, dict):
            return "corrupt metadata: top-level JSON must be an object"

        if metadata.get("metadata_version") != CACHE_METADATA_VERSION:
            return "unsupported metadata version"

        if metadata.get("writer_version") != CACHE_WRITER_VERSION:
            return "unsupported writer version"

        if metadata.get("source_key") != display_key:
            return "source key mismatch"

        if source_sql is not None and metadata.get("source_sql") != source_sql:
            return "source SQL mismatch"

        if "source_version" not in metadata:
            return "corrupt metadata: missing source version"

        if metadata.get("source_version") != source_version:
            return "stale source version"

        parquet_meta = metadata.get("parquet")
        if not isinstance(parquet_meta, dict):
            return "corrupt metadata: missing parquet section"

        if parquet_meta.get("file_name") != cache_path.name:
            return "parquet file name mismatch"

        try:
            stat = cache_path.stat()
        except FileNotFoundError:
            return "incomplete entry: missing parquet"

        if stat.st_size <= 0:
            return "incomplete entry: empty parquet"

        if parquet_meta.get("size_bytes") != stat.st_size:
            return "parquet size mismatch"

        expected_sha = parquet_meta.get("sha256")
        if not isinstance(expected_sha, str) or len(expected_sha) != 64:
            return "corrupt metadata: invalid parquet checksum"

        try:
            actual_sha = self._file_hash(cache_path)
        except OSError as exc:
            return f"cannot read parquet bytes: {exc}"

        if actual_sha != expected_sha:
            return "parquet checksum mismatch"

        data_meta = metadata.get("data")
        if not isinstance(data_meta, dict):
            return "corrupt metadata: missing data section"

        try:
            parquet_file = pq.ParquetFile(str(cache_path))
        except Exception as exc:
            return f"corrupt parquet: unreadable metadata: {exc}"

        row_count = data_meta.get("row_count")
        if row_count != parquet_file.metadata.num_rows:
            return "parquet row count mismatch"

        schema_metadata = self._schema_metadata(parquet_file.schema_arrow)
        if data_meta.get("column_count") != len(schema_metadata):
            return "parquet column count mismatch"

        if data_meta.get("schema_hash") != self._schema_hash(schema_metadata):
            return "parquet schema hash mismatch"

        return None

    @staticmethod
    def _version_preview(source_version: str | None) -> str:
        if source_version is None:
            return ""

        if source_version.startswith("sha256:"):
            return f" sha={source_version.removeprefix('sha256:')[:40]}"

        return f" version={source_version[:40]}"

    @staticmethod
    def _parquet_display_key(filename: object) -> str:
        return os.fspath(filename)

    def _cache_path(self, display_key: str) -> Path:
        key_path = Path(display_key)
        if key_path.is_absolute():
            digest = hashlib.sha256(display_key.encode("utf-8")).hexdigest()
            return self.cache_dir / "_absolute" / digest / key_path.name

        if ".." in key_path.parts:
            digest = hashlib.sha256(display_key.encode("utf-8")).hexdigest()
            suffix = key_path.suffix or ".parquet"
            return self.cache_dir / "_relative" / f"{digest}{suffix}"

        return self.cache_dir.joinpath(*key_path.parts)

    def _part_path(self, path: Path) -> Path:
        return path.with_name(
            f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.part"
        )

    @staticmethod
    def _schema_metadata(schema: object) -> list[dict[str, object]]:
        return [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ]

    @staticmethod
    def _schema_hash(schema_metadata: list[dict[str, object]]) -> str:
        encoded = json.dumps(
            schema_metadata,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _filename_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        bound = signature.bind_partial(*args, **kwargs)
        return bound.arguments["filename"]

    @staticmethod
    def _replace_filename_arg(
        signature: inspect.Signature,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        filename: Path,
    ) -> tuple[tuple[object, ...], dict[str, object]]:
        bound = signature.bind_partial(*args, **kwargs)
        bound.arguments["filename"] = filename
        return bound.args, bound.kwargs

    @staticmethod
    def _is_complete(path: Path) -> bool:
        try:
            return path.stat().st_size > 0
        except FileNotFoundError:
            return False

    @staticmethod
    def _remove_file(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
