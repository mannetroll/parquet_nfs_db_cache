import argparse
import oracledb
import polars as pl

from database.oracle_env import apply_dotenv
from database.oracle_pool import make_pool_factory
from main import nfscache
from nfscache.data.data_container import DataContainer

DEFAULT_BATCH_SIZE = 10000


def oracle_args() -> argparse.Namespace:
    args = argparse.Namespace(
        host="localhost",
        port=1521,
        service="FREEPDB1",
        user="SOMEUSER",
        password="cache",
        batch_size=DEFAULT_BATCH_SIZE,
    )
    apply_dotenv(args)
    return args


def connect(args: argparse.Namespace) -> oracledb.Connection:
    dsn = f"{args.host}:{args.port}/{args.service}"
    return oracledb.connect(
        user=args.user,
        password=args.password,
        dsn=dsn,
    )


def _fetch_data_container(
    connection: oracledb.Connection,
    sql: str,
    *,
    batch_size: int,
) -> DataContainer:
    batches: list[pl.DataFrame] = []
    with connection.cursor() as cursor:
        cursor.arraysize = batch_size
        cursor.execute(sql)
        headers = tuple(column[0] for column in cursor.description)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batches.append(pl.DataFrame(rows, schema=headers, orient="row"))

    df = pl.concat(batches) if batches else pl.DataFrame(schema=headers)
    return DataContainer({"headers": headers, "data": df})


# Pool-backed factory: the version probe (every call) and the cold-load fetch
# both borrow from one process-local pool instead of opening a fresh connection.
nfscache.connect_factory = make_pool_factory(oracle_args())


@nfscache.sql
def read_data_container(sql: str) -> DataContainer:
    # This body only runs on a cache miss, so reaching it means a live read.
    print(f"Serving from Oracle (cache miss): {sql}", flush=True)
    args = oracle_args()
    with nfscache.connect_factory() as connection:
        return _fetch_data_container(
            connection,
            sql,
            batch_size=int(args.batch_size),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Oracle SQL into a DataContainer."
    )
    parser.add_argument("sql")
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
    # Go through the cache so the run reports Oracle (miss) vs cache (hit).
    container = read_data_container(args.sql)
    print("table:", container.data.rows_data_pl)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
