import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.nfs_cache import NFSCache


class FakeCursor:
    """Minimal cursor honoring the VERSION_SQL contract used by the cache."""

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
    """Mutable stand-in for Oracle so tests drive the version token directly."""

    def __init__(self, *, n_rows: int = 2, scn: int | None = 100) -> None:
        self.n_rows = n_rows
        self.scn = scn

    def connect_factory(self) -> FakeConnection:
        return FakeConnection(self)


class NFSCacheMetadataTests(unittest.TestCase):
    def test_metadata_contains_authoritative_fields_and_warms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(tmp_path / "cache", connect_factory=oracle.connect_factory)
            calls = 0

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                nonlocal calls
                calls += 1
                table = pa.table({"id": [1, 2], "name": ["a", "b"]})
                pq.write_table(table, parquet_path)

            stream("select * from DEMO", tmp_path / "a.parquet", object())
            stream("select * from DEMO", tmp_path / "b.parquet", object())

            self.assertEqual(calls, 1)
            metadata_text = self._only_metadata_path(tmp_path / "cache").read_text(
                encoding="utf-8"
            )
            self.assertIn('\n  "created_at":', metadata_text)
            self.assertTrue(metadata_text.endswith("\n"))
            metadata = self._read_only_metadata(tmp_path / "cache")

            self.assertEqual(metadata["metadata_version"], 1)
            self.assertTrue(str(metadata["source_key"]).startswith("sql/DEMO/"))
            self.assertEqual(metadata["source_version"], "DEMO@SCN:100|ROWS:2")
            self.assertEqual(metadata["source_sql"], "select * from DEMO")
            data_metadata = metadata["data"]
            parquet_metadata = metadata["parquet"]
            self.assertEqual(data_metadata["row_count"], 2)
            self.assertEqual(data_metadata["column_count"], 2)
            self.assertEqual(len(data_metadata["schema_hash"]), 64)
            self.assertGreater(parquet_metadata["size_bytes"], 0)
            self.assertEqual(len(parquet_metadata["sha256"]), 64)
            self.assertIn("created_at", metadata)
            self.assertIn("writer_version", metadata)

    def test_corrupt_metadata_reloads_and_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(tmp_path / "cache", connect_factory=oracle.connect_factory)
            calls = 0

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                nonlocal calls
                calls += 1
                pq.write_table(pa.table({"value": [calls]}), parquet_path)

            stream("select * from T", tmp_path / "a.parquet", object())
            meta_path = self._only_metadata_path(tmp_path / "cache")
            meta_path.write_text("{not-json", encoding="utf-8")

            out = stream("select * from T", tmp_path / "b.parquet", object())

            self.assertEqual(calls, 2)
            self.assertEqual(pq.read_table(out).to_pydict(), {"value": [2]})
            data_metadata = self._read_only_metadata(tmp_path / "cache")["data"]
            self.assertEqual(data_metadata["row_count"], 1)

    def test_corrupt_parquet_reloads_and_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(tmp_path / "cache", connect_factory=oracle.connect_factory)
            calls = 0

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                nonlocal calls
                calls += 1
                pq.write_table(pa.table({"value": [calls]}), parquet_path)

            stream("select * from T", tmp_path / "a.parquet", object())
            meta_path = self._only_metadata_path(tmp_path / "cache")
            parquet_path = meta_path.with_name(
                meta_path.name.removesuffix(".meta.json")
            )
            parquet_path.write_bytes(b"not parquet")

            out = stream("select * from T", tmp_path / "b.parquet", object())

            self.assertEqual(calls, 2)
            self.assertEqual(pq.read_table(out).to_pydict(), {"value": [2]})

    def test_sql_parquet_cache_uses_path_arg_on_method(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(tmp_path / "cache", connect_factory=oracle.connect_factory)
            calls = 0

            class Loader:
                @cache.sql_parquet
                def stream(
                    self, sql: str, parquet_path: Path, connection: object
                ) -> None:
                    nonlocal calls
                    calls += 1
                    pq.write_table(pa.table({"value": [calls]}), parquet_path)

            loader = Loader()
            loader.stream("select * from T", tmp_path / "a.parquet", object())
            out = loader.stream("select * from T", tmp_path / "b.parquet", object())

            self.assertEqual(calls, 1)
            self.assertEqual(pq.read_table(out).to_pydict(), {"value": [1]})
            metadata = self._read_only_metadata(tmp_path / "cache")
            self.assertTrue(str(metadata["source_key"]).startswith("sql/T/"))

    def test_sql_cache_metadata_contains_normalized_sql(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(tmp_path / "cache", connect_factory=oracle.connect_factory)
            raw_sql = " select  *\nfrom DATA_DEMO where id = 1; "
            normalized_sql = "select * from DATA_DEMO where id = 1"

            @cache.sql_parquet
            def stream(sql: str, parquet_path: Path, connection: object) -> None:
                pq.write_table(pa.table({"id": [1]}), parquet_path)

            stream(raw_sql, tmp_path / "a.parquet", object())
            metadata = self._read_only_metadata(tmp_path / "cache")

            self.assertEqual(metadata["source_sql"], normalized_sql)

    @staticmethod
    def _only_metadata_path(cache_dir: Path) -> Path:
        paths = list(cache_dir.rglob("*.meta.json"))
        if len(paths) != 1:
            raise AssertionError(f"expected one metadata file, got {paths}")
        return paths[0]

    def _read_only_metadata(self, cache_dir: Path) -> dict[str, object]:
        return json.loads(
            self._only_metadata_path(cache_dir).read_text(encoding="utf-8")
        )


if __name__ == "__main__":
    unittest.main()
