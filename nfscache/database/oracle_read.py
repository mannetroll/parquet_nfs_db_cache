import argparse
import sys

import oracledb
import pyarrow as pa

from nfscache.database.oracle_arrow import rows_to_table
from nfscache.database.oracle_arrow import schema_from_description
from nfscache.database.oracle_env import apply_dotenv

DEFAULT_BATCH_SIZE = 10000


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


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

        schema = schema_from_description(cursor.description)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batches.append(rows_to_table(rows, schema))

    if batches:
        return pa.concat_tables(batches)
    return rows_to_table([], schema)


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
