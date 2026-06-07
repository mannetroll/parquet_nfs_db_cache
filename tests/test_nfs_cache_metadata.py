from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import polars as pl

from disk_cache.data.data_container import DataContainer
from disk_cache.nfs_cache import DBCache


class DBCacheMetadataTests(unittest.TestCase):
    def test_metadata_contains_authoritative_fields_and_warms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "source.txt"
            source_path.write_text("v1", encoding="utf-8")
            cache = DBCache(tmp_path / "cache")
            calls = 0

            @cache.data_container_cache
            def load(path: Path) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load(source_path)
            load(source_path)

            self.assertEqual(calls, 1)
            metadata_text = self._only_metadata_path(tmp_path / "cache").read_text(
                encoding="utf-8"
            )
            self.assertIn('\n  "created_at":', metadata_text)
            self.assertTrue(metadata_text.endswith("\n"))
            metadata = self._read_only_metadata(tmp_path / "cache")

            self.assertEqual(metadata["metadata_version"], 1)
            self.assertEqual(metadata["source_key"], str(source_path))
            self.assertTrue(str(metadata["source_version"]).startswith("sha256:"))
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
            source_path = tmp_path / "source.txt"
            source_path.write_text("v1", encoding="utf-8")
            cache = DBCache(tmp_path / "cache")
            calls = 0

            @cache.data_container_cache
            def load(path: Path) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load(source_path)
            meta_path = self._only_metadata_path(tmp_path / "cache")
            meta_path.write_text("{not-json", encoding="utf-8")

            data = load(source_path)
            df = data.data.rows_data_pl

            self.assertEqual(calls, 2)
            self.assertEqual(df["value"].to_list(), [2])
            data_metadata = self._read_only_metadata(tmp_path / "cache")["data"]
            self.assertEqual(data_metadata["row_count"], 1)

    def test_corrupt_parquet_reloads_and_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "source.txt"
            source_path.write_text("v1", encoding="utf-8")
            cache = DBCache(tmp_path / "cache")
            calls = 0

            @cache.data_container_cache
            def load(path: Path) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load(source_path)
            meta_path = self._only_metadata_path(tmp_path / "cache")
            parquet_path = meta_path.with_name(
                meta_path.name.removesuffix(".meta.json")
            )
            parquet_path.write_bytes(b"not parquet")

            data = load(source_path)
            df = data.data.rows_data_pl

            self.assertEqual(calls, 2)
            self.assertEqual(df["value"].to_list(), [2])

    def test_sql_cache_metadata_contains_normalized_sql(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cache = DBCache(tmp_path / "cache")
            raw_sql = " select  *\nfrom DATA_CONTAINER_DEMO where id = 1; "
            normalized_sql = "select * from DATA_CONTAINER_DEMO where id = 1"

            @cache.sql
            def load(sql: str) -> DataContainer:
                df = pl.DataFrame({"id": [1]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load(raw_sql)
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
