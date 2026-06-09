import argparse
import datetime as dt
import decimal
import json
import sys
from typing import Any

import oracledb
import pyarrow as pa

from nfscache.database.oracle_env import apply_dotenv

DEFAULT_BATCH_SIZE = 10000

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


def read_table(
    connection: oracledb.Connection,
    sql: str,
    *,
    batch_size: int,
) -> pa.Table:
    """Read an Oracle result set into a PyArrow Table."""
    batches: list[pa.Table] = []
    with connection.cursor() as cursor:
        cursor.arraysize = batch_size
        cursor.execute(sql)
        if cursor.description is None:
            raise ValueError("SQL did not return a result set")

        schema = _schema_from_description(cursor.description)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batches.append(_rows_to_table(rows, schema))

    if batches:
        return pa.concat_tables(batches)
    return _rows_to_table([], schema)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Oracle SQL into a PyArrow Table and log it."
    )
    parser.add_argument("sql")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1521)
    parser.add_argument("--service", default="FREEPDB1")
    parser.add_argument("--user", default="SOMEUSER")
    parser.add_argument("--password", default="cache")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()
    apply_dotenv(args)
    return args


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args()
    print(f"Reading from Oracle: {args.sql}", flush=True)
    with connect(args) as connection:
        table = read_table(connection, args.sql, batch_size=int(args.batch_size))

    print(f"rows={table.num_rows} cols={table.num_columns}")
    print("table:", table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
