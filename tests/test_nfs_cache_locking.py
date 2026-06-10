import json
import os
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.nfs_cache import NFSCache


class FakeCursor:
    def __init__(self, source: "FakeOracle") -> None:
        self._source = source

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        return None

    def fetchone(self) -> tuple[int, int | None]:
        return (self._source.n_rows, self._source.scn)


class FakeConnection:
    def __init__(self, source: "FakeOracle") -> None:
        self._source = source

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._source)


class FakeOracle:
    """Stable version token so concurrent gets are warm hits, not reloads."""

    def __init__(self, *, n_rows: int = 2, scn: int | None = 100) -> None:
        self.n_rows = n_rows
        self.scn = scn

    def connect_factory(self) -> FakeConnection:
        return FakeConnection(self)


class ObservedReadCache(NFSCache):
    def __init__(self, cache_dir: Path, connect_factory: Callable[[], object]) -> None:
        super().__init__(
            cache_dir,
            poll_seconds=0.005,
            connect_factory=connect_factory,
        )
        self.active_readers = 0
        self.max_active_readers = 0
        self.reader_lock = threading.Lock()

    def _cached_parquet_path_if_valid(self, *args: object, **kwargs: object):
        result = super()._cached_parquet_path_if_valid(*args, **kwargs)
        if result is None:
            return None
        # Warm hit: this runs while the per-key read lock is held, so counting
        # here proves multiple readers overlap on a warm cache entry.
        with self.reader_lock:
            self.active_readers += 1
            self.max_active_readers = max(
                self.max_active_readers,
                self.active_readers,
            )
        try:
            time.sleep(0.05)
            return result
        finally:
            with self.reader_lock:
                self.active_readers -= 1


class MissingInitialReaderMetadataDirCache(NFSCache):
    def _start_lock_heartbeat(
        self,
        lock_path: Path,
        lock_type: str,
        *,
        create_missing: bool = False,
    ):
        if lock_type == "reader" and create_missing:
            self._remove_lock_dir(lock_path)
        return super()._start_lock_heartbeat(
            lock_path,
            lock_type,
            create_missing=create_missing,
        )


class BrokenReaderMetadataCache(NFSCache):
    def _write_lock_metadata(
        self,
        lock_path: Path,
        lock_type: str,
        lock_uuid: str,
        created_at: str,
        *,
        create_missing: bool = False,
    ) -> None:
        if lock_type == "reader":
            raise PermissionError("metadata write failed")
        super()._write_lock_metadata(
            lock_path,
            lock_type,
            lock_uuid,
            created_at,
            create_missing=create_missing,
        )


class CopyLockObservingCache(NFSCache):
    """Records how many reader tokens are held when the validated file is copied,
    proving the export runs while the read lock is still held."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.readers_during_copy: int | None = None

    def _copy_cached_parquet(self, cache_path: Path, output_path: Path) -> None:
        readers_dir = cache_path.with_name(cache_path.name + ".lock") / "readers"
        if readers_dir.exists():
            tokens = [token for token in readers_dir.iterdir() if token.is_dir()]
        else:
            tokens = []
        self.readers_during_copy = len(tokens)
        return super()._copy_cached_parquet(cache_path, output_path)


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

    def test_reader_metadata_write_recreates_missing_token_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = MissingInitialReaderMetadataDirCache(
                Path(tmp) / "cache",
                poll_seconds=0.005,
            )
            lock_path = Path(tmp) / "entry.parquet.lock"

            reader_lease = cache._acquire_read_lock(lock_path)
            try:
                self.assertTrue((reader_lease.path / "lock.json").is_file())
            finally:
                cache._release_read_lock(reader_lease)

    def test_reader_metadata_write_reraises_non_transient_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = BrokenReaderMetadataCache(
                Path(tmp) / "cache",
                poll_seconds=0.005,
            )
            lock_path = Path(tmp) / "entry.parquet.lock"

            with self.assertRaises(PermissionError):
                cache._acquire_read_lock(lock_path)

    def test_warm_cache_reads_can_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = ObservedReadCache(tmp_path / "cache", oracle.connect_factory)
            cold_loads = 0

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                nonlocal cold_loads
                cold_loads += 1
                pq.write_table(pa.table({"value": [1, 2, 3]}), parquet_path)

            stream("select * from T", tmp_path / "warm.parquet", object())

            with ThreadPoolExecutor(max_workers=4) as executor:
                list(
                    executor.map(
                        lambda i: stream(
                            "select * from T", tmp_path / f"out_{i}.parquet", object()
                        ),
                        range(4),
                    )
                )

            self.assertEqual(cold_loads, 1)
            self.assertGreaterEqual(cache.max_active_readers, 2)

    def test_warm_hit_copies_under_read_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = CopyLockObservingCache(
                tmp_path / "cache",
                connect_factory=oracle.connect_factory,
                poll_seconds=0.005,
            )

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                pq.write_table(pa.table({"value": [1, 2, 3]}), parquet_path)

            stream("select * from T", tmp_path / "cold.parquet", object())
            cache.readers_during_copy = None
            stream("select * from T", tmp_path / "warm.parquet", object())

            # The warm-hit export ran while this client's read token was held,
            # so a concurrent writer could not replace the file mid-copy.
            self.assertEqual(cache.readers_during_copy, 1)

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
