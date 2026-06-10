import argparse
import os
import re
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from uuid import uuid4

import oracledb
import pyarrow.compute as pc
import pyarrow.parquet as pq

from nfscache.database.oracle_arrow import rows_to_table
from nfscache.database.oracle_arrow import schema_from_description
from nfscache.database.oracle_env import apply_dotenv
from nfscache.database.oracle_pool import make_pool_factory
from nfscache.database.oracle_streaming import DEFAULT_BATCH_SIZE
from nfscache.database.oracle_streaming import DEFAULT_COMPRESSION
from nfscache.nfs_parquet_cache import NFSParquetCache

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


def generation_wave_steps(*, gets_per_client: int, generations: int) -> set[int]:
    return {
        max(1, min(gets_per_client, round((i * gets_per_client) / generations)))
        for i in range(1, generations + 1)
    }


def _stream_to_parquet(
    sql_query: str,
    parquet_path: str | os.PathLike,
    connection: oracledb.Connection,
    *,
    batch_size: int,
) -> None:
    """Stream an Oracle result set into one Parquet file at ``parquet_path``.

    Mirrors ``oracle_streaming.stream_data_to_parquet`` but parameterizes the
    batch size; the cache decorator hands it a ``*.part`` path to write into.
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
            cursor.arraysize = batch_size
            cursor.execute(sql_query)
            if cursor.description is None:
                raise ValueError("SQL did not return a result set")

            schema = schema_from_description(cursor.description)
            writer = pq.ParquetWriter(
                str(part_path), schema, compression=DEFAULT_COMPRESSION
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                table = rows_to_table(rows, schema)
                writer.write_table(table)
                row_count += table.num_rows

            if row_count == 0:
                writer.write_table(rows_to_table([], schema))

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


def load_parquet_cache(
    sql: str,
    output_path: Path,
    cache_dir: Path,
    batch_size: int,
) -> Path:
    args = oracle_args(batch_size=batch_size)
    # One process-local pool, memoized per process so it is built once. The
    # single acquired connection serves both the cache's SCN version probe (run
    # inside the decorator) and the cold stream.
    factory = make_pool_factory(args)
    nfscache = NFSParquetCache(cache_dir)

    @nfscache.sql_parquet
    def stream(sql_query: str, parquet_path: Path, connection: object) -> None:
        print(
            f"[pid {os.getpid()}] Streaming Oracle -> {parquet_path}: {sql_query}",
            flush=True,
        )
        _stream_to_parquet(
            sql_query, parquet_path, connection, batch_size=batch_size
        )

    with factory() as connection:
        return stream(sql, output_path, connection)


def get_once(
    client_id: int,
    get_no: int,
    sql: str,
    out_dir: Path,
    cache_dir: Path,
    batch_size: int,
) -> tuple[str, int, int, int, int, int, tuple[int, ...]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"client_{client_id}.parquet"
    result_path = load_parquet_cache(sql, output_path, cache_dir, batch_size)

    table = pq.read_table(result_path)
    generations: tuple[int, ...]
    if "GENERATION" in table.column_names:
        unique = pc.unique(table.column("GENERATION"))
        generations = tuple(sorted(int(value) for value in unique.to_pylist()))
    else:
        generations = tuple()

    return (
        "client",
        client_id,
        os.getpid(),
        get_no,
        table.num_rows,
        table.num_columns,
        generations,
    )


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


def run_get_round(
    *,
    clients: int,
    sql: str,
    out_dir: Path,
    cache_dir: Path,
    batch_size: int,
) -> None:
    print(f"[swarm-stream] Final warm check: {clients} clients", flush=True)
    with ProcessPoolExecutor(max_workers=clients) as executor:
        futures = [
            executor.submit(
                get_once, client_id, 0, sql, out_dir, cache_dir, batch_size
            )
            for client_id in range(1, clients + 1)
        ]
        results = [future.result() for future in futures]

    summary = ", ".join(
        f"client={client_id} pid={pid} rows={rows} cols={cols} gen={generations}"
        for _, client_id, pid, _, rows, cols, generations in results
    )
    print(f"[swarm-stream] Final warm check done: {summary}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Hammer the sql_parquet (streaming) cache while the source changes."
    )
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--writers", type=int, default=1)
    parser.add_argument("--gets-per-client", type=int, default=12)
    parser.add_argument("--generations", type=int, default=6)
    parser.add_argument("--n-rows", type=int, default=8192)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--table", default="SWARM_STREAM_DATA")
    parser.add_argument("--out-dir", type=Path, default=Path("__cache__/swarm_stream_out"))
    parser.add_argument("--cache-dir", type=Path, default=Path("__cache__/swarm_stream"))
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
        f"[swarm-stream] Initialized table={table_name} "
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
                    args.out_dir,
                    args.cache_dir,
                    int(args.batch_size),
                )
                for client_id in range(1, args.clients + 1)
            )

            for future in futures:
                result = future.result()
                print(
                    f"[swarm-stream] wave={get_no}/{args.gets_per_client} "
                    f"result={result}",
                    flush=True,
                )

    run_get_round(
        clients=args.clients,
        sql=sql,
        out_dir=args.out_dir,
        cache_dir=args.cache_dir,
        batch_size=int(args.batch_size),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
