from __future__ import annotations

import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from nfs_cache.data.data_container import DataContainer
from nfs_cache.db_cache import DBCache


class ObservedReadCache(DBCache):
    def __init__(self, cache_dir: Path) -> None:
        super().__init__(cache_dir, poll_seconds=0.005)
        self.active_readers = 0
        self.max_active_readers = 0
        self.reader_lock = threading.Lock()

    def _read_data_container(self, cache_path: Path) -> DataContainer:
        with self.reader_lock:
            self.active_readers += 1
            self.max_active_readers = max(
                self.max_active_readers,
                self.active_readers,
            )
        try:
            time.sleep(0.05)
            return super()._read_data_container(cache_path)
        finally:
            with self.reader_lock:
                self.active_readers -= 1


class DBCacheLockingTests(unittest.TestCase):
    def test_warm_cache_reads_can_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "source.txt"
            source_path.write_text("v1", encoding="utf-8")
            cache = ObservedReadCache(tmp_path / "cache")
            cold_loads = 0

            @cache.data_container_cache
            def load(path: Path) -> DataContainer:
                nonlocal cold_loads
                cold_loads += 1
                df = pl.DataFrame({"value": [1, 2, 3]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load(source_path)

            with ThreadPoolExecutor(max_workers=4) as executor:
                list(executor.map(lambda _: load(source_path), range(4)))

            self.assertEqual(cold_loads, 1)
            self.assertGreaterEqual(cache.max_active_readers, 2)

    def test_writer_intent_blocks_new_readers_until_existing_readers_finish(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = DBCache(Path(tmp) / "cache", poll_seconds=0.005)
            lock_path = Path(tmp) / "entry.parquet.lock"
            first_reader = cache._acquire_read_lock(lock_path)
            writer_acquired = threading.Event()
            release_writer = threading.Event()
            second_reader_acquired = threading.Event()

            def writer() -> None:
                writer_path = cache._acquire_write_lock(lock_path)
                try:
                    writer_acquired.set()
                    release_writer.wait(timeout=2)
                finally:
                    cache._release_write_lock(writer_path)

            def second_reader() -> None:
                reader_path = cache._acquire_read_lock(lock_path)
                try:
                    second_reader_acquired.set()
                finally:
                    cache._release_read_lock(reader_path)

            writer_thread = threading.Thread(target=writer)
            second_reader_thread = threading.Thread(target=second_reader)

            try:
                writer_thread.start()
                self._wait_until(lambda: (lock_path / "writer").exists())

                second_reader_thread.start()
                time.sleep(0.05)
                self.assertFalse(writer_acquired.is_set())
                self.assertFalse(second_reader_acquired.is_set())

                cache._release_read_lock(first_reader)
                self.assertTrue(writer_acquired.wait(timeout=1))
                time.sleep(0.05)
                self.assertFalse(second_reader_acquired.is_set())

                release_writer.set()
                self.assertTrue(second_reader_acquired.wait(timeout=1))
            finally:
                cache._release_read_lock(first_reader)
                release_writer.set()
                writer_thread.join(timeout=1)
                second_reader_thread.join(timeout=1)

    def _wait_until(self, predicate: Callable[[], bool], timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.005)
        self.fail("condition was not satisfied before timeout")


if __name__ == "__main__":
    unittest.main()
