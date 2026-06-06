from __future__ import annotations

import functools
import hashlib
import os
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import ParamSpec

import polars as pl
import pyarrow.parquet as pq

from nfs_cache.data.data_container import DataContainer

P = ParamSpec("P")


class DBCache:
    def __init__(self, cache_dir: Path, *, poll_seconds: float = 0.1) -> None:
        self.cache_dir = cache_dir
        self.poll_seconds = poll_seconds

    def data_container_cache(
        self,
        func: Callable[P, DataContainer],
    ) -> Callable[P, DataContainer]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> DataContainer:
            display_key = self._display_key(func, tuple(args), dict(kwargs))
            cache_path = self._cache_path(display_key)
            part_path = cache_path.with_name(f"{cache_path.name}.part")
            lock_path = cache_path.with_name(f"{cache_path.name}.lock")

            cached = self._read_if_cached(cache_path, display_key)
            if cached is not None:
                return cached

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            while True:
                try:
                    lock_path.mkdir()
                    break
                except FileExistsError:
                    cached = self._wait_for_cached(cache_path, lock_path, display_key)
                    if cached is not None:
                        return cached

            try:
                cached = self._read_if_cached(cache_path, display_key)
                if cached is not None:
                    return cached

                self._remove_file(part_path)
                part_path.touch()
                try:
                    data = func(*args, **kwargs)
                    self._write_data_container(part_path, cache_path, data)
                    return data
                except Exception:
                    self._remove_file(part_path)
                    raise
            finally:
                try:
                    lock_path.rmdir()
                except FileNotFoundError:
                    pass

        return wrapper

    def _read_if_cached(self, cache_path: Path, display_key: str) -> DataContainer | None:
        if not self._is_complete(cache_path):
            return None

        print(f"Returning cached object: {display_key}...")
        return self._read_data_container(cache_path)

    def _wait_for_cached(
        self,
        cache_path: Path,
        lock_path: Path,
        display_key: str,
    ) -> DataContainer | None:
        while lock_path.exists():
            cached = self._read_if_cached(cache_path, display_key)
            if cached is not None:
                return cached
            time.sleep(self.poll_seconds)

        return self._read_if_cached(cache_path, display_key)

    def _read_data_container(self, cache_path: Path) -> DataContainer:
        df = pl.read_parquet(cache_path)
        return DataContainer({"headers": tuple(df.columns), "data": df})

    def _write_data_container(
        self,
        part_path: Path,
        cache_path: Path,
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
            os.replace(part_path, cache_path)
        except Exception:
            self._remove_file(part_path)
            raise

    def _display_key(
        self,
        func: Callable[..., object],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> str:
        path_arg = args[0] if args else kwargs.get("path")
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
