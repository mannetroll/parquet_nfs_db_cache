from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from disk_cache.data.data_container import DataContainer
from disk_cache.nfs_cache import NFSCache


class ObservedReadCache(NFSCache):
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


class NFSCacheLockingTests(unittest.TestCase):
    def test_lock_metadata_is_written_for_reader_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = NFSCache(Path(tmp) / "cache", poll_seconds=0.005)
            lock_path = Path(tmp) / "entry.parquet.lock"
            reader_lease = cache._acquire_read_lock(lock_path)
            try:
                metadata = json.loads(
                    (reader_lease.path / "lock.json").read_text(encoding="utf-8")
                )
                self.assertEqual(metadata["lock_type"], "reader")
                self.assertEqual(metadata["pid"], os.getpid())
                self.assertIn("hostname", metadata)
                self.assertIn("uuid", metadata)
                self.assertIn("created_at", metadata)
                self.assertIn("last_seen", metadata)
            finally:
                cache._release_read_lock(reader_lease)

    def test_warm_cache_reads_can_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "source.txt"
            source_path.write_text("v1", encoding="utf-8")
            cache = ObservedReadCache(tmp_path / "cache")
            cold_loads = 0

            @cache.parquet
            def load(filename: Path) -> DataContainer:
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
            cache = NFSCache(Path(tmp) / "cache", poll_seconds=0.005)
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

    def test_stale_writer_intent_is_broken_for_new_reader(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = NFSCache(
                Path(tmp) / "cache",
                poll_seconds=0.005,
                stale_lock_seconds=0.01,
                heartbeat_seconds=0.005,
            )
            lock_path = Path(tmp) / "entry.parquet.lock"
            writer_path = lock_path / "writer"
            self._make_stale_lock(writer_path, "writer")

            reader_lease = cache._acquire_read_lock(lock_path)
            try:
                self.assertFalse(writer_path.exists())
                self.assertTrue(reader_lease.path.exists())
            finally:
                cache._release_read_lock(reader_lease)

    def test_stale_reader_token_is_broken_for_writer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = NFSCache(
                Path(tmp) / "cache",
                poll_seconds=0.005,
                stale_lock_seconds=0.01,
                heartbeat_seconds=0.005,
            )
            lock_path = Path(tmp) / "entry.parquet.lock"
            reader_path = lock_path / "readers" / "abandoned.reader"
            self._make_stale_lock(reader_path, "reader")

            writer_lease = cache._acquire_write_lock(lock_path)
            try:
                self.assertFalse(reader_path.exists())
                self.assertTrue(writer_lease.path.exists())
            finally:
                cache._release_write_lock(writer_lease)

    def test_heartbeat_keeps_live_lock_from_becoming_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = NFSCache(
                Path(tmp) / "cache",
                poll_seconds=0.005,
                stale_lock_seconds=0.2,
                heartbeat_seconds=0.02,
            )
            lock_path = Path(tmp) / "entry.parquet.lock"
            reader_lease = cache._acquire_read_lock(lock_path)
            try:
                time.sleep(0.25)
                self.assertFalse(cache._break_stale_lock(reader_lease.path))
                self.assertTrue(reader_lease.path.exists())
            finally:
                cache._release_read_lock(reader_lease)

    def test_release_keeps_shared_lock_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = NFSCache(Path(tmp) / "cache", poll_seconds=0.005)
            lock_path = Path(tmp) / "entry.parquet.lock"

            reader_lease = cache._acquire_read_lock(lock_path)
            cache._release_read_lock(reader_lease)
            self.assertTrue(lock_path.is_dir())
            self.assertTrue((lock_path / "readers").is_dir())
            self.assertFalse(reader_lease.path.exists())

            writer_lease = cache._acquire_write_lock(lock_path)
            cache._release_write_lock(writer_lease)
            self.assertTrue(lock_path.is_dir())
            self.assertTrue((lock_path / "readers").is_dir())
            self.assertFalse(writer_lease.path.exists())

    def _wait_until(self, predicate: Callable[[], bool], timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.005)
        self.fail("condition was not satisfied before timeout")

    @staticmethod
    def _make_stale_lock(lock_path: Path, lock_type: str) -> None:
        lock_path.mkdir(parents=True)
        metadata_path = lock_path / "lock.json"
        metadata_path.write_text(
            json.dumps(
                {
                    "metadata_version": 1,
                    "lock_type": lock_type,
                    "hostname": "dead-host",
                    "pid": 999999,
                    "uuid": "deadbeef",
                    "created_at": "2000-01-01T00:00:00+00:00",
                    "last_seen": "2000-01-01T00:00:00+00:00",
                },
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        old_time = time.time() - 60
        os.utime(metadata_path, (old_time, old_time))
        os.utime(lock_path, (old_time, old_time))


if __name__ == "__main__":
    unittest.main()
