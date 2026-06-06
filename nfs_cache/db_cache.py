from __future__ import annotations

import functools
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import ParamSpec

import polars as pl
import pyarrow.parquet as pq

from nfs_cache.data.data_container import DataContainer

P = ParamSpec("P")


class DBCache:
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
        source_version: Callable[..., object] | None = None,
        connect_factory: Callable[[], object] | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.poll_seconds = poll_seconds
        self.source_version = source_version
        # Opaque callable returning a DB connection (anything with a
        # context-manager `cursor()`); used by `sql_container_cache` to read the
        # source version. Kept generic so the cache does not depend on oracledb.
        self.connect_factory = connect_factory

    def data_container_cache(
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            display_key = self._display_key(func, args_tuple, kwargs_dict)
            return self._run_cached(
                display_key,
                lambda: self._source_version(args_tuple, kwargs_dict),
                lambda: func(*args, **kwargs),
            )

        return wrapper

    def sql_container_cache(
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            sql = args[0] if args else kwargs["sql"]
            return_cols = kwargs.get("return_cols")
            display_key = self._sql_display_key(str(sql), return_cols)
            return self._run_cached(
                display_key,
                lambda: self._sql_source_version(str(sql)),
                lambda: func(*args, **kwargs),
            )

        return wrapper

    def _run_cached(
        self,
        display_key: str,
        version_fn: Callable[[], str | None],
        load_fn: Callable[[], DataContainer],
    ) -> DataContainer:
        cache_path = self._cache_path(display_key)
        meta_path = cache_path.with_name(f"{cache_path.name}.meta.json")
        lock_path = cache_path.with_name(f"{cache_path.name}.lock")

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire_lock(lock_path)
        try:
            source_version = version_fn()
            cached = self._read_if_cached(
                cache_path,
                meta_path,
                display_key,
                source_version,
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
                    source_version,
                    data,
                )
                return data
            except Exception:
                self._remove_file(part_path)
                self._remove_file(meta_part_path)
                raise
        finally:
            try:
                lock_path.rmdir()
            except FileNotFoundError:
                pass

    def _acquire_lock(self, lock_path: Path) -> None:
        while True:
            try:
                lock_path.mkdir()
                return
            except FileExistsError:
                time.sleep(self.poll_seconds)

    def _read_if_cached(
        self,
        cache_path: Path,
        meta_path: Path,
        display_key: str,
        source_version: str | None,
    ) -> DataContainer | None:
        if not self._is_complete(cache_path):
            return None

        if not self._is_current(meta_path, source_version):
            return None

        version_preview = self._version_preview(source_version)
        print(
            f"Returning cached object: {display_key}{version_preview}...",
            flush=True,
        )
        try:
            return self._read_data_container(cache_path)
        except FileNotFoundError:
            return None

    def _read_data_container(self, cache_path: Path) -> DataContainer:
        df = pl.read_parquet(cache_path)
        return DataContainer({"headers": tuple(df.columns), "data": df})

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
        source_version: str | None,
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
            self._write_metadata(meta_part_path, source_version)
            os.replace(part_path, cache_path)
            if source_version is not None:
                os.replace(meta_part_path, meta_path)
        except Exception:
            self._remove_file(part_path)
            self._remove_file(meta_part_path)
            raise

    def _write_metadata(
        self,
        meta_part_path: Path,
        source_version: str | None,
    ) -> None:
        if source_version is None:
            return

        metadata = {"source_version": source_version}
        meta_part_path.write_text(
            json.dumps(metadata, sort_keys=True),
            encoding="utf-8",
        )

    def _source_version(
        self,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> str | None:
        if self.source_version is not None:
            version = self.source_version(*args, **kwargs)
            return None if version is None else str(version)

        path_arg = self._path_arg(args, kwargs)
        if isinstance(path_arg, (str, os.PathLike)):
            path = Path(path_arg)
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
        ).hexdigest()
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

    def _is_current(self, meta_path: Path, source_version: str | None) -> bool:
        if source_version is None:
            return True

        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return False

        return metadata.get("source_version") == source_version

    @staticmethod
    def _version_preview(source_version: str | None) -> str:
        if source_version is None:
            return ""

        if source_version.startswith("sha256:"):
            return f" sha={source_version.removeprefix('sha256:')[:40]}"

        return f" version={source_version[:40]}"

    def _display_key(
        self,
        func: Callable[..., object],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> str:
        path_arg = self._path_arg(args, kwargs)
        if isinstance(path_arg, (str, os.PathLike)):
            return os.fspath(path_arg)

        cache_key = repr(
            (func.__module__, func.__qualname__, args, sorted(kwargs.items()))
        )
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return f"{func.__module__}.{func.__qualname__}/{digest}.parquet"

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
    def _path_arg(
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object | None:
        return args[0] if args else kwargs.get("path")

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
