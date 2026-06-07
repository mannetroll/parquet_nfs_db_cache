from __future__ import annotations

import argparse
import os
import re
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import oracledb
import polars as pl

from database.oracle_env import apply_dotenv
from disk_cache.data.data_container import DataContainer
from disk_cache.nfs_cache import NFSCache

IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")


def oracle_identifier(name: str) -> str:
    if not IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid Oracle identifier: {name!r}")
    return name.upper()


def oracle_args(*, batch_size: int) -> argparse.Namespace:
    args = argparse.Namespace(
        host="localhost",
        port=1521,
        service="FREEPDB1",
        user="SOMEUSER",
        password="cache",
        batch_size=batch_size,
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


def create_table(connection: oracledb.Connection, table_name: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            create table {table_name} (
              generation number(10) not null,
              row_id number(10) not null,
              value number(10) not null,
              payload varchar2(64) not null
            )
            """
        )


def setup_table(table_name: str, *, n_rows: int, batch_size: int) -> int:
    args = oracle_args(batch_size=batch_size)
    with connect(args) as connection:
        drop_table_if_exists(connection, table_name)
        create_table(connection, table_name)
        write_table_generation(
            table_name,
            generation_no=0,
            n_rows=n_rows,
            batch_size=batch_size,
        )
        return current_scn(connection)


def write_table_generation(
    table_name: str,
    *,
    generation_no: int,
    n_rows: int,
    batch_size: int,
) -> int:
    args = oracle_args(batch_size=batch_size)
    with connect(args) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"delete from {table_name}")
            insert_sql = (
                f"insert into {table_name} "
                "(generation, row_id, value, payload) values (:1, :2, :3, :4)"
            )
            for start in range(0, n_rows, batch_size):
                stop = min(start + batch_size, n_rows)
                rows = [
                    (
                        generation_no,
                        row_id,
                        (generation_no * n_rows) + row_id,
                        f"g{generation_no}-row{row_id}",
                    )
                    for row_id in range(start, stop)
                ]
                cursor.executemany(insert_sql, rows)
        connection.commit()
        return current_scn(connection)


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


def load_data_container(sql: str, cache_dir: Path, batch_size: int) -> DataContainer:
    nfscache = NFSCache(cache_dir)
    nfscache.connect_factory = lambda: connect(oracle_args(batch_size=batch_size))

    @nfscache.sql
    def load(sql: str) -> DataContainer:
        print(f"[pid {os.getpid()}] Reading Oracle: {sql}", flush=True)
        args = oracle_args(batch_size=batch_size)
        with connect(args) as connection:
            return _fetch_data_container(connection, sql, batch_size=batch_size)

    return load(sql)


def get_once(
    client_id: int,
    get_no: int,
    sql: str,
    cache_dir: Path,
    batch_size: int,
) -> tuple[str, int, int, int, int, int, tuple[int, ...]]:
    data = load_data_container(sql, cache_dir, batch_size)
    df = data.data.rows_data_pl
    if not isinstance(df, pl.DataFrame):
        raise TypeError("DataContainer.data.rows_data_pl must be a Polars DataFrame")

    generations: tuple[int, ...]
    if "GENERATION" in df.columns:
        generations = tuple(int(value) for value in df["GENERATION"].unique().sort())
    else:
        generations = tuple()

    return "client", client_id, os.getpid(), get_no, df.height, df.width, generations


def write_once(
    writer_id: int,
    generation_no: int,
    *,
    table_name: str,
    n_rows: int,
    batch_size: int,
    generation_total: int,
) -> tuple[str, int, int, int, int, int]:
    scn = write_table_generation(
        table_name,
        generation_no=generation_no,
        n_rows=n_rows,
        batch_size=batch_size,
    )
    print(
        f"[writer {writer_id} pid {os.getpid()}] generation "
        f"{generation_no}/{generation_total}: table={table_name} scn={scn}",
        flush=True,
    )
    return "writer", writer_id, os.getpid(), generation_no, n_rows, scn


def generation_wave_steps(*, gets_per_client: int, generations: int) -> set[int]:
    return {
        max(1, min(gets_per_client, round((i * gets_per_client) / generations)))
        for i in range(1, generations + 1)
    }


def run_get_round(
    *,
    clients: int,
    sql: str,
    cache_dir: Path,
    batch_size: int,
) -> None:
    print(f"[swarm-sql] Final warm check: {clients} clients", flush=True)
    with ProcessPoolExecutor(max_workers=clients) as executor:
        futures = [
            executor.submit(get_once, client_id, 0, sql, cache_dir, batch_size)
            for client_id in range(1, clients + 1)
        ]
        results = [future.result() for future in futures]

    summary = ", ".join(
        f"client={client_id} pid={pid} rows={rows} cols={cols} gen={generations}"
        for _, client_id, pid, _, rows, cols, generations in results
    )
    print(f"[swarm-sql] Final warm check done: {summary}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--writers", type=int, default=1)
    parser.add_argument("--gets-per-client", type=int, default=12)
    parser.add_argument("--generations", type=int, default=6)
    parser.add_argument("--n-rows", type=int, default=8192)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--table", default="SWARM_SQL_DATA")
    parser.add_argument("--cache-dir", type=Path, default=Path("__cache__/swarm_sql"))
    args = parser.parse_args()
    apply_dotenv(args)

    if args.clients < 1:
        parser.error("--clients must be >= 1")
    if args.writers < 1:
        parser.error("--writers must be >= 1")
    if args.gets_per_client < 1:
        parser.error("--gets-per-client must be >= 1")
    if args.generations < 1:
        parser.error("--generations must be >= 1")
    if args.n_rows < 1:
        parser.error("--n-rows must be >= 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")

    table_name = oracle_identifier(args.table)
    initial_scn = setup_table(
        table_name,
        n_rows=int(args.n_rows),
        batch_size=int(args.batch_size),
    )
    print(
        f"[swarm-sql] Initialized table={table_name} "
        f"rows={args.n_rows} scn={initial_scn}",
        flush=True,
    )

    sql = f"select * from {table_name} order by row_id"
    max_workers = args.clients + args.writers
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        generation_steps = generation_wave_steps(
            gets_per_client=args.gets_per_client,
            generations=args.generations,
        )
        generation_no = 0
        for get_no in range(1, args.gets_per_client + 1):
            futures: list[Future[tuple[object, ...]]] = []

            if get_no in generation_steps:
                for writer_id in range(1, args.writers + 1):
                    generation_no += 1
                    futures.append(
                        executor.submit(
                            write_once,
                            writer_id,
                            generation_no,
                            table_name=table_name,
                            n_rows=int(args.n_rows),
                            batch_size=int(args.batch_size),
                            generation_total=args.generations * args.writers,
                        )
                    )

            futures.extend(
                executor.submit(
                    get_once,
                    client_id,
                    get_no,
                    sql,
                    args.cache_dir,
                    int(args.batch_size),
                )
                for client_id in range(1, args.clients + 1)
            )

            for future in futures:
                result = future.result()
                print(
                    f"[swarm-sql] wave={get_no}/{args.gets_per_client} "
                    f"result={result}",
                    flush=True,
                )

    run_get_round(
        clients=args.clients,
        sql=sql,
        cache_dir=args.cache_dir,
        batch_size=int(args.batch_size),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
