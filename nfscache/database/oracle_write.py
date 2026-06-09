import argparse
import re
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from nfscache.database.oracle_env import apply_dotenv

IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")


def oracle_identifier(name: str) -> str:
    if not IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Oracle identifier: {name!r}")
    return name.upper()


def table_name_from_path(path: Path) -> str:
    return oracle_identifier(path.stem)


def oracle_type(dtype: pa.DataType) -> str:
    if pa.types.is_integer(dtype):
        return "NUMBER(38)"
    if pa.types.is_floating(dtype) or pa.types.is_decimal(dtype):
        return "BINARY_DOUBLE"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "VARCHAR2(4000)"
    if pa.types.is_boolean(dtype):
        return "NUMBER(1)"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    return "CLOB"


def read_table(path: Path) -> pa.Table:
    print(f"Reading: {path}...")
    return pq.read_table(path)


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


def current_scn(connection: oracledb.Connection) -> int:
    with connection.cursor() as cursor:
        scn, = cursor.execute("select current_scn from v$database").fetchone()
    return int(scn)


def drop_table_if_exists(connection: oracledb.Connection, table_name: str) -> None:
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"drop table {table_name} purge")
        except oracledb.DatabaseError as exc:
            error, = exc.args
            if getattr(error, "code", None) != 942:
                raise


def create_table(
    connection: oracledb.Connection,
    table_name: str,
    table: pa.Table,
) -> list[str]:
    columns = [oracle_identifier(field.name) for field in table.schema]
    definitions = [
        f"{column} {oracle_type(field.type)}"
        for column, field in zip(columns, table.schema, strict=True)
    ]
    ddl = f"create table {table_name} ({', '.join(definitions)})"
    with connection.cursor() as cursor:
        cursor.execute(ddl)
    return columns


def insert_table(
    connection: oracledb.Connection,
    table_name: str,
    table: pa.Table,
    *,
    batch_size: int,
) -> tuple[int, int]:
    columns = create_table(connection, table_name, table)
    placeholders = ", ".join(f":{index}" for index in range(1, len(columns) + 1))
    sql = (
        f"insert into {table_name} ({', '.join(columns)}) "
        f"values ({placeholders})"
    )

    inserted = 0
    with connection.cursor() as cursor:
        for batch in table.to_batches(max_chunksize=batch_size):
            if batch.num_rows == 0:
                continue
            rows = list(zip(*(column.to_pylist() for column in batch.columns)))
            cursor.executemany(sql, rows)
            inserted += len(rows)
    connection.commit()
    return inserted, len(columns)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read a parquet file and write it to Oracle."
    )
    parser.add_argument("parquet_path", type=Path)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1521)
    parser.add_argument("--service", default="FREEPDB1")
    parser.add_argument("--user", default="SOMEUSER")
    parser.add_argument("--password", default="cache")
    parser.add_argument(
        "--table",
        help="Oracle table name. Defaults to the parquet file stem.",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    apply_dotenv(args)
    return args


def main() -> int:
    args = parse_args()
    path = args.parquet_path
    table_name = (
        oracle_identifier(args.table)
        if args.table is not None
        else table_name_from_path(path)
    )
    table = read_table(path)

    print(f"Table: rows={table.num_rows} cols={table.num_columns}")
    with connect(args) as connection:
        before_scn = current_scn(connection)
        print(f"Oracle current_scn before write: {before_scn}")
        drop_table_if_exists(connection, table_name)
        rows, cols = insert_table(
            connection,
            table_name,
            table,
            batch_size=int(args.batch_size),
        )
        after_scn = current_scn(connection)

    print(f"Wrote parquet to {args.user.upper()}.{table_name}: rows={rows} cols={cols}")
    print(f"Oracle current_scn after write: {after_scn}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
