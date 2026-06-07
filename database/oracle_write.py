import argparse
import re
from pathlib import Path

import oracledb
import polars as pl

from database.oracle_env import apply_dotenv
from nfscache.data.data_container import DataContainer

IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")

INTEGER_TYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}
FLOAT_TYPES = {pl.Float32, pl.Float64}
STRING_TYPES = {pl.String, pl.Categorical, pl.Enum}


def oracle_identifier(name: str) -> str:
    if not IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Oracle identifier: {name!r}")
    return name.upper()


def table_name_from_path(path: Path) -> str:
    return oracle_identifier(path.stem)


def oracle_type(dtype: pl.DataType) -> str:
    if dtype in INTEGER_TYPES:
        return "NUMBER(38)"
    if dtype in FLOAT_TYPES:
        return "BINARY_DOUBLE"
    if dtype in STRING_TYPES:
        return "VARCHAR2(4000)"
    if dtype == pl.Boolean:
        return "NUMBER(1)"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Datetime:
        return "TIMESTAMP"
    return "CLOB"


def read_data_container(path: Path) -> DataContainer:
    print(f"Reading: {path}...")
    df = pl.read_parquet(path)
    return DataContainer({"headers": tuple(df.columns), "data": df})


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
    df: pl.DataFrame,
) -> list[str]:
    columns = [oracle_identifier(column) for column in df.columns]
    definitions = [
        f"{column} {oracle_type(dtype)}"
        for column, dtype in zip(columns, df.dtypes, strict=True)
    ]
    ddl = f"create table {table_name} ({', '.join(definitions)})"
    with connection.cursor() as cursor:
        cursor.execute(ddl)
    return columns


def insert_data_container(
    connection: oracledb.Connection,
    table_name: str,
    data_container: DataContainer,
    *,
    batch_size: int,
) -> tuple[int, int]:
    df = data_container.data.rows_data_pl
    if not isinstance(df, pl.DataFrame):
        raise TypeError("DataContainer.data.rows_data_pl must be a Polars DataFrame")

    columns = create_table(connection, table_name, df)
    placeholders = ", ".join(f":{index}" for index in range(1, len(columns) + 1))
    sql = (
        f"insert into {table_name} ({', '.join(columns)}) "
        f"values ({placeholders})"
    )

    inserted = 0
    with connection.cursor() as cursor:
        for batch in df.iter_slices(n_rows=batch_size):
            rows = list(batch.iter_rows(named=False))
            if not rows:
                continue
            cursor.executemany(sql, rows)
            inserted += len(rows)
    connection.commit()
    return inserted, len(columns)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read a parquet-backed DataContainer and write it to Oracle."
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
    data_container = read_data_container(path)
    df = data_container.data.rows_data_pl
    if not isinstance(df, pl.DataFrame):
        raise TypeError("DataContainer.data.rows_data_pl must be a Polars DataFrame")

    print(f"DataContainer: rows={df.height} cols={df.width}")
    with connect(args) as connection:
        before_scn = current_scn(connection)
        print(f"Oracle current_scn before write: {before_scn}")
        drop_table_if_exists(connection, table_name)
        rows, cols = insert_data_container(
            connection,
            table_name,
            data_container,
            batch_size=int(args.batch_size),
        )
        after_scn = current_scn(connection)

    print(f"Wrote DataContainer to {args.user.upper()}.{table_name}: rows={rows} cols={cols}")
    print(f"Oracle current_scn after write: {after_scn}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
