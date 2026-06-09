import argparse
import os
from pathlib import Path
from uuid import uuid4

import oracledb
import pyarrow.parquet as pq

from nfscache.database.oracle_arrow import rows_to_table
from nfscache.database.oracle_arrow import schema_from_description
from nfscache.database.oracle_env import apply_dotenv
from nfscache.nfs_cache import NFSCache

CACHE_ROOT = Path("__cache__")
DEFAULT_BATCH_SIZE = 100000
DEFAULT_COMPRESSION = "snappy"
DEFAULT_SQL = "select * from A_TEST_1048576"
DEFAULT_OUTPUT = Path("A_TEST_1048576.parquet")
nfscache = NFSCache(CACHE_ROOT / "nfs")


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


@nfscache.sql_parquet
def stream_data_to_parquet(
    sql_query: str,
    parquet_path: str | os.PathLike,
    connection: oracledb.Connection,
):
    """Stream an Oracle result set to one Parquet file.
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

            schema = schema_from_description(cursor.description)
            writer = pq.ParquetWriter(
                str(part_path),
                schema,
                compression=DEFAULT_COMPRESSION,
            )

            while True:
                rows = cursor.fetchmany(DEFAULT_BATCH_SIZE)
                if not rows:
                    break

                table = rows_to_table(rows, schema)
                writer.write_table(table)
                row_count += table.num_rows
                print(f"Wrote {row_count} rows", end="\r")

            if row_count == 0:
                writer.write_table(rows_to_table([], schema))

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
