from __future__ import annotations

import functools
import hashlib
import json
import os
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
    def __init__(
        self,
        cache_dir: Path,
        *,
        poll_seconds: float = 0.1,
        source_version: Callable[..., object] | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.poll_seconds = poll_seconds
        self.source_version = source_version

    def data_container_cache(
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            args_tuple = tuple(args)
            kwargs_dict = dict(kwargs)
            display_key = self._display_key(func, args_tuple, kwargs_dict)
            source_version = self._source_version(args_tuple, kwargs_dict)
            cache_path = self._cache_path(display_key)
            meta_path = cache_path.with_name(f"{cache_path.name}.meta.json")
            lock_path = cache_path.with_name(f"{cache_path.name}.lock")

            cached = self._read_if_cached(
                cache_path,
                meta_path,
                display_key,
                source_version,
            )
            if cached is not None:
                return cached

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            while True:
                try:
                    lock_path.mkdir()
                    break
                except FileExistsError:
                    cached = self._wait_for_cached(
                        cache_path,
                        meta_path,
                        lock_path,
                        display_key,
                        source_version,
                    )
                    if cached is not None:
                        return cached

            try:
                cached = self._read_if_cached(
                    cache_path,
                    meta_path,
                    display_key,
                    source_version,
                )
                if cached is not None:
                    return cached

                self._remove_file(cache_path)
                self._remove_file(meta_path)
                part_path = self._part_path(cache_path)
                meta_part_path = self._part_path(meta_path)
                try:
                    data = func(*args, **kwargs)
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

        return wrapper

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

        print(f"Returning cached object: {display_key}...")
        return self._read_data_container(cache_path)

    def _wait_for_cached(
        self,
        cache_path: Path,
        meta_path: Path,
        lock_path: Path,
        display_key: str,
        source_version: str | None,
    ) -> DataContainer | None:
        while lock_path.exists():
            cached = self._read_if_cached(
                cache_path,
                meta_path,
                display_key,
                source_version,
            )
            if cached is not None:
                return cached
            time.sleep(self.poll_seconds)

        return self._read_if_cached(
            cache_path,
            meta_path,
            display_key,
            source_version,
        )

    def _read_data_container(self, cache_path: Path) -> DataContainer:
        df = pl.read_parquet(cache_path)
        return DataContainer({"headers": tuple(df.columns), "data": df})

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

    def _is_current(self, meta_path: Path, source_version: str | None) -> bool:
        if source_version is None:
            return True

        try:
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return False

        return metadata.get("source_version") == source_version

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
