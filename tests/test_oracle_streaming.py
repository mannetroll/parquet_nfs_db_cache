import decimal
import tempfile
import unittest
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.database.oracle_streaming import (
    DEFAULT_BATCH_SIZE,
    stream_data_to_parquet,
)

raw_stream_data_to_parquet = stream_data_to_parquet.__wrapped__


def _desc(
    name: str,
    type_code: object,
    *,
    precision: int | None = None,
    scale: int | None = None,
) -> tuple[object, ...]:
    return (name, type_code, None, None, precision, scale, True)


class FakeCursor:
    def __init__(
        self,
        description: tuple[tuple[object, ...], ...] | None,
        batches: list[list[tuple[object, ...]]],
    ) -> None:
        self.description = description
        self.batches = list(batches)
        self.arraysize = None
        self.executed_sql = None
        self.fetch_sizes: list[int] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

    def fetchmany(self, batch_size: int) -> list[tuple[object, ...]]:
        self.fetch_sizes.append(batch_size)
        if self.batches:
            return self.batches.pop(0)
        return []


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self) -> FakeCursor:
        return self.cursor_obj


class OracleStreamingTests(unittest.TestCase):
    def test_streams_multiple_batches_to_one_parquet_file(self) -> None:
        description = (
            _desc("ID", oracledb.DB_TYPE_NUMBER, precision=10, scale=0),
            _desc("NAME", oracledb.DB_TYPE_VARCHAR),
            _desc("AMOUNT", oracledb.DB_TYPE_NUMBER, precision=10, scale=2),
        )
        cursor = FakeCursor(
            description,
            [
                [
                    (1, "alpha", decimal.Decimal("1.25")),
                    (2, "beta", decimal.Decimal("2.50")),
                ],
                [(3, "gamma", decimal.Decimal("3.75"))],
            ],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "streamed.parquet"
            raw_stream_data_to_parquet(
                "select * from demo",
                path,
                FakeConnection(cursor),
            )

            table = pq.read_table(path)
            self.assertEqual((table.num_rows, table.num_columns), (3, 3))
            self.assertEqual(cursor.executed_sql, "select * from demo")
            self.assertEqual(cursor.arraysize, DEFAULT_BATCH_SIZE)
            self.assertEqual(
                cursor.fetch_sizes,
                [DEFAULT_BATCH_SIZE] * 3,
            )
            self.assertEqual(
                table.to_pydict(),
                {
                    "ID": [1, 2, 3],
                    "NAME": ["alpha", "beta", "gamma"],
                    "AMOUNT": [1.25, 2.5, 3.75],
                },
            )
            self.assertEqual(list(path.parent.glob(f"{path.name}.*.part")), [])

    def test_empty_result_writes_schema_only_parquet(self) -> None:
        description = (
            _desc("ID", oracledb.DB_TYPE_NUMBER, precision=10, scale=0),
            _desc("NAME", oracledb.DB_TYPE_VARCHAR),
        )
        cursor = FakeCursor(description, [])

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"
            raw_stream_data_to_parquet(
                "select * from empty_demo",
                path,
                FakeConnection(cursor),
            )

            table = pq.read_table(path)
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(table.num_columns, 2)
            self.assertEqual(table.column_names, ["ID", "NAME"])
            self.assertEqual(table.schema.field("ID").type, pa.int64())
            self.assertEqual(table.schema.field("NAME").type, pa.string())

    def test_non_query_sql_is_rejected_without_output_file(self) -> None:
        cursor = FakeCursor(None, [])

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "no_result.parquet"
            with self.assertRaisesRegex(ValueError, "result set"):
                raw_stream_data_to_parquet(
                    "begin null; end;",
                    path,
                    FakeConnection(cursor),
                )

            self.assertFalse(path.exists())
            self.assertEqual(list(path.parent.glob(f"{path.name}.*.part")), [])


if __name__ == "__main__":
    unittest.main()
