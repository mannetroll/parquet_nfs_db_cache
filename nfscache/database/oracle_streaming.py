import argparse
import datetime as dt
import decimal
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.database.oracle_env import apply_dotenv
from nfscache.nfs_cache import NFSCache

CACHE_ROOT = Path("__cache__")
DEFAULT_BATCH_SIZE = 100000
DEFAULT_COMPRESSION = "snappy"
DEFAULT_SQL = "select * from A_TEST_1048576"
DEFAULT_OUTPUT = Path("A_TEST_1048576.parquet")
nfscache = NFSCache(CACHE_ROOT / "nfs")

TEXT_TYPES = {
    oracledb.DB_TYPE_CHAR,
    oracledb.DB_TYPE_CLOB,
    oracledb.DB_TYPE_JSON,
    oracledb.DB_TYPE_LONG,
    oracledb.DB_TYPE_LONG_NVARCHAR,
    oracledb.DB_TYPE_NCHAR,
    oracledb.DB_TYPE_NCLOB,
    oracledb.DB_TYPE_NVARCHAR,
    oracledb.DB_TYPE_ROWID,
    oracledb.DB_TYPE_UROWID,
    oracledb.DB_TYPE_VARCHAR,
    oracledb.DB_TYPE_XMLTYPE,
}
BINARY_TYPES = {
    oracledb.DB_TYPE_BFILE,
    oracledb.DB_TYPE_BLOB,
    oracledb.DB_TYPE_LONG_RAW,
    oracledb.DB_TYPE_RAW,
}
TIMESTAMP_TYPES = {
    oracledb.DB_TYPE_DATE,
    oracledb.DB_TYPE_TIMESTAMP,
    oracledb.DB_TYPE_TIMESTAMP_LTZ,
    oracledb.DB_TYPE_TIMESTAMP_TZ,
}


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


def _description_value(column: object, name: str, index: int) -> object:
    if hasattr(column, name):
        return getattr(column, name)
    return column[index]


def _column_name(column: object) -> str:
    return str(_description_value(column, "name", 0))


def _column_type(column: object) -> object:
    type_code = getattr(column, "type_code", None)
    if type_code is not None:
        return type_code
    return _description_value(column, "type", 1)


def _column_precision(column: object) -> int | None:
    try:
        value = _description_value(column, "precision", 4)
    except (IndexError, TypeError):
        return None
    return int(value) if value is not None else None


def _column_scale(column: object) -> int | None:
    try:
        value = _description_value(column, "scale", 5)
    except (IndexError, TypeError):
        return None
    return int(value) if value is not None else None


def _number_type(column: object) -> pa.DataType:
    precision = _column_precision(column)
    scale = _column_scale(column)

    if scale == 0:
        if precision is not None and 18 < precision <= 38:
            return pa.decimal128(precision, 0)
        return pa.int64()

    if (
        oracledb.defaults.fetch_decimals
        and precision is not None
        and 0 <= precision <= 38
        and scale is not None
        and scale >= 0
    ):
        return pa.decimal128(max(precision, 1), scale)

    return pa.float64()


def _arrow_type(column: object) -> pa.DataType:
    db_type = _column_type(column)

    if db_type in TEXT_TYPES:
        return pa.string()
    if db_type in BINARY_TYPES:
        return pa.binary()
    if db_type == oracledb.DB_TYPE_BOOLEAN:
        return pa.bool_()
    if db_type in {
        oracledb.DB_TYPE_BINARY_DOUBLE,
        oracledb.DB_TYPE_BINARY_FLOAT,
    }:
        return pa.float64()
    if db_type in {
        oracledb.DB_TYPE_BINARY_INTEGER,
        oracledb.DB_TYPE_NUMBER,
    }:
        return _number_type(column)
    if db_type in TIMESTAMP_TYPES:
        return pa.timestamp("us")
    if db_type == oracledb.DB_TYPE_INTERVAL_DS:
        return pa.duration("us")

    return pa.string()


def _schema_from_description(description: object) -> pa.Schema:
    return pa.schema(
        pa.field(_column_name(column), _arrow_type(column))
        for column in description
    )


def _lob_value(value: object) -> object:
    if hasattr(value, "read"):
        return value.read()
    return value


def _coerce_value(value: object, arrow_type: pa.DataType) -> object:
    if value is None:
        return None

    value = _lob_value(value)

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, bytearray):
            return bytes(value)
        return value

    if pa.types.is_floating(arrow_type):
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value

    if pa.types.is_integer(arrow_type):
        if isinstance(value, decimal.Decimal):
            return int(value)
        return value

    if pa.types.is_decimal(arrow_type):
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(str(value))

    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            return dt.datetime.combine(value, dt.time())
        return value

    return value


def _rows_to_table(rows: list[tuple[Any, ...]], schema: pa.Schema) -> pa.Table:
    arrays = []
    for column_index, field in enumerate(schema):
        values = [
            _coerce_value(row[column_index], field.type)
            for row in rows
        ]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


@nfscache.sql_parquet
def stream_data_to_parquet(
    sql_query: str,
    parquet_path: str | os.PathLike,
    connection: oracledb.Connection,
) -> tuple[int, int]:
    """Stream an Oracle result set to one Parquet file.

    Returns `(row_count, column_count)`.
    """
    output_path = Path(parquet_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = output_path.with_name(
        f"{output_path.name}.{os.getpid()}.{uuid4().hex}.part"
    )

    row_count = 0
    writer: pq.ParquetWriter | None = None

    try:
        with connection.cursor() as cursor:
            cursor.arraysize = DEFAULT_BATCH_SIZE
            cursor.execute(sql_query)

            if cursor.description is None:
                raise ValueError("SQL did not return a result set")

            schema = _schema_from_description(cursor.description)
            writer = pq.ParquetWriter(
                str(part_path),
                schema,
                compression=DEFAULT_COMPRESSION,
            )

            while True:
                rows = cursor.fetchmany(DEFAULT_BATCH_SIZE)
                if not rows:
                    break

                table = _rows_to_table(rows, schema)
                writer.write_table(table)
                row_count += table.num_rows
                print(f"Wrote {row_count} rows", end="\r")

            if row_count == 0:
                writer.write_table(_rows_to_table([], schema))

        if writer is not None:
            writer.close()
            writer = None
        os.replace(part_path, output_path)
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        try:
            part_path.unlink()
        except FileNotFoundError:
            pass
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream an Oracle SQL result set directly to Parquet."
    )
    parser.add_argument("sql", nargs="?", default=DEFAULT_SQL)
    parser.add_argument("output", nargs="?", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1521)
    parser.add_argument("--service", default="FREEPDB1")
    parser.add_argument("--user", default="SOMEUSER")
    parser.add_argument("--password", default="cache")
    args = parser.parse_args()
    apply_dotenv(args)
    return args


def main() -> int:
    args = parse_args()
    nfscache.connect_factory = lambda: connect(args)

    with connect(args) as connection:
        output_path = stream_data_to_parquet(
            sql_query=args.sql,
            parquet_path=args.output,
            connection=connection,
        )

    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
