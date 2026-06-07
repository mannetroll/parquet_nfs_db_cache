import tempfile
import unittest
from pathlib import Path

import polars as pl

from disk_cache.data.data_container import DataContainer
from disk_cache.nfs_cache import NFSCache


class FakeCursor:
    """Minimal cursor honoring the VERSION_SQL contract used by the cache."""

    def __init__(self, source: "FakeOracle") -> None:
        self._source = source

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self._source.version_queries.append(sql)

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
        self.connects = 0
        self.version_queries: list[str] = []

    def connect_factory(self) -> FakeConnection:
        self.connects += 1
        return FakeConnection(self)


class NFSCacheSqlVersionTests(unittest.TestCase):
    def test_table_from_sql_parses_first_from(self) -> None:
        cache = NFSCache(Path("unused"))
        self.assertEqual(cache._table_from_sql("select * from FOO"), "FOO")
        self.assertEqual(
            cache._table_from_sql("SELECT a, b FROM my.schema_tbl WHERE a = 1"),
            "my.schema_tbl",
        )
        self.assertEqual(
            cache._table_from_sql('select * from "Quoted_Tbl"'),
            "Quoted_Tbl",
        )
        self.assertEqual(cache._table_from_sql("select 1 from dual"), "dual")
        self.assertIsNone(cache._table_from_sql("select 1"))

    def test_normalize_sql_collapses_whitespace_and_semicolon(self) -> None:
        normalized = NFSCache._normalize_sql(" select  *\nfrom  T where id = 1; ")
        self.assertEqual(normalized, "select * from T where id = 1")

    def test_display_key_includes_table_and_is_stable(self) -> None:
        cache = NFSCache(Path("unused"))
        key_a = cache._sql_display_key("select * from orders")
        key_b = cache._sql_display_key("select  *  from   orders")
        self.assertEqual(key_a, key_b)
        self.assertTrue(key_a.startswith("sql/ORDERS/"))
        self.assertTrue(key_a.endswith(".parquet"))

    def test_display_key_varies_with_sql_and_return_cols(self) -> None:
        cache = NFSCache(Path("unused"))
        base = cache._sql_display_key("select * from t")
        other_sql = cache._sql_display_key("select id from t")
        with_cols = cache._sql_display_key("select * from t", return_cols=["A", "B"])
        cols_reordered = cache._sql_display_key(
            "select * from t", return_cols=["b", "a"]
        )

        self.assertNotEqual(base, other_sql)
        self.assertNotEqual(base, with_cols)
        # return_cols are order- and case-insensitive in the key.
        self.assertEqual(with_cols, cols_reordered)

    def test_source_version_disabled_without_connect_factory(self) -> None:
        cache = NFSCache(Path("unused"))
        self.assertIsNone(cache._sql_source_version("select * from t"))

    def test_source_version_uses_scn_and_row_count(self) -> None:
        oracle = FakeOracle(n_rows=7, scn=4242)
        cache = NFSCache(Path("unused"), connect_factory=oracle.connect_factory)
        version = cache._sql_source_version("select * from MYTBL")
        self.assertEqual(version, "MYTBL@SCN:4242|ROWS:7")
        self.assertEqual(len(oracle.version_queries), 1)
        self.assertIn("MYTBL", oracle.version_queries[0])

    def test_source_version_handles_null_scn(self) -> None:
        oracle = FakeOracle(n_rows=0, scn=None)
        cache = NFSCache(Path("unused"), connect_factory=oracle.connect_factory)
        self.assertEqual(
            cache._sql_source_version("select * from EMPTY_TBL"),
            "EMPTY_TBL@SCN:0|ROWS:0",
        )


class NFSCacheSqlFlowTests(unittest.TestCase):
    def test_warm_hit_skips_reload_when_version_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(
                Path(tmp) / "cache",
                connect_factory=oracle.connect_factory,
            )
            calls = 0

            @cache.sql
            def load(sql: str) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            first = load("select * from T")
            second = load("select * from T")

            self.assertEqual(calls, 1)
            self.assertEqual(first.data.rows_data_pl["value"].to_list(), [1])
            self.assertEqual(second.data.rows_data_pl["value"].to_list(), [1])

    def test_scn_change_invalidates_and_reloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(
                Path(tmp) / "cache",
                connect_factory=oracle.connect_factory,
            )
            calls = 0

            @cache.sql
            def load(sql: str) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load("select * from T")
            oracle.scn = 200  # table changed -> version token advances

            data = load("select * from T")

            self.assertEqual(calls, 2)
            self.assertEqual(data.data.rows_data_pl["value"].to_list(), [2])

    def test_row_count_change_invalidates_even_if_scn_static(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(
                Path(tmp) / "cache",
                connect_factory=oracle.connect_factory,
            )
            calls = 0

            @cache.sql
            def load(sql: str) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load("select * from T")
            oracle.n_rows = 5  # SCN unchanged; row-count guard must still invalidate

            load("select * from T")

            self.assertEqual(calls, 2)

    def test_distinct_sql_use_distinct_cache_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            oracle = FakeOracle(n_rows=2, scn=100)
            cache = NFSCache(
                Path(tmp) / "cache",
                connect_factory=oracle.connect_factory,
            )
            calls = 0

            @cache.sql
            def load(sql: str) -> DataContainer:
                nonlocal calls
                calls += 1
                df = pl.DataFrame({"value": [calls]})
                return DataContainer({"headers": tuple(df.columns), "data": df})

            load("select * from T")
            load("select id from T")
            load("select * from T")  # warm hit on the first entry

            self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
