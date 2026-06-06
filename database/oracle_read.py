from __future__ import annotations

import argparse
import re

import oracledb
import polars as pl

from database.oracle_env import apply_dotenv
from nfs_cache.data.data_container import DataContainer

IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")


def oracle_identifier(name: str) -> str:
    if not IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Oracle identifier: {name!r}")
    return name.upper()


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


def read_data_container(
    connection: oracledb.Connection,
    table_name: str,
    *,
    batch_size: int,
) -> DataContainer:
    batches: list[pl.DataFrame] = []
    with connection.cursor() as cursor:
        cursor.arraysize = batch_size
        cursor.execute(f"select * from {table_name}")
        headers = tuple(column[0] for column in cursor.description)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batches.append(pl.DataFrame(rows, schema=headers, orient="row"))

    df = pl.concat(batches) if batches else pl.DataFrame(schema=headers)
    return DataContainer({"headers": headers, "data": df})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read an Oracle table into a DataContainer."
    )
    parser.add_argument("table")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1521)
    parser.add_argument("--service", default="FREEPDB1")
    parser.add_argument("--user", default="SOMEUSER")
    parser.add_argument("--password", default="cache")
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()
    apply_dotenv(args)
    return args


def main() -> int:
    args = parse_args()
    table_name = oracle_identifier(args.table)
    with connect(args) as connection:
        container = read_data_container(
            connection,
            table_name,
            batch_size=int(args.batch_size),
        )

    print("table:", container.data.rows_data_pl)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
