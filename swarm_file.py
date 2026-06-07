from __future__ import annotations

import argparse
import hashlib
import os
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import polars as pl

from disk_cache.data.data_container import DataContainer
from disk_cache.nfs_cache import DBCache
from disk_cache.util.generate_parquets import ensure_one_parquet


def load_data_container(path: Path, cache_dir: Path) -> DataContainer:
    dbcache = DBCache(cache_dir)

    @dbcache.data_container_cache
    def load(path: Path) -> DataContainer:
        print(f"[pid {os.getpid()}] Reading: {path}...", flush=True)
        df = pl.read_parquet(path)
        return DataContainer({"headers": tuple(df.columns), "data": df})

    return load(path)


def get_once(client_id: int, path: Path, cache_dir: Path) -> tuple[int, int, int, int]:
    data = load_data_container(path, cache_dir)
    df = data.data.rows_data_pl
    if not isinstance(df, pl.DataFrame):
        raise TypeError("DataContainer.data.rows_data_pl must be a Polars DataFrame")

    return client_id, os.getpid(), df.height, df.width


def generate_parquet(
    *,
    data_dir: Path,
    n_rows: int,
    n_cols: int,
    n_int_cols: int,
    n_str_cols: int,
) -> Path:
    path = data_dir / f"A_SWARM_TEST_{n_rows}.parquet"
    print(f"[swarm] Generating: {path}...", flush=True)
    path = ensure_one_parquet(
        out_dir=data_dir,
        base_name=f"SWARM_TEST_{n_rows}.parquet",
        prefix="A_",
        n_rows=n_rows,
        n_cols=n_cols,
        seed=None,
        float_scale=5.0,
        n_int_cols=n_int_cols,
        n_str_cols=n_str_cols,
    )
    print(f"[swarm] Generated: {path} sha={file_sha(path)[:40]}...", flush=True)
    return path


def generate_once(
    generator_id: int,
    generation_no: int,
    *,
    data_dir: Path,
    n_rows: int,
    n_cols: int,
    n_int_cols: int,
    n_str_cols: int,
    generation_total: int,
) -> tuple[str, int, int, int, str]:
    pid = os.getpid()
    path = generate_parquet(
        data_dir=data_dir,
        n_rows=n_rows,
        n_cols=n_cols,
        n_int_cols=n_int_cols,
        n_str_cols=n_str_cols,
    )
    print(
        f"[generator {generator_id} pid {pid}] generation "
        f"{generation_no}/{generation_total}: {path}",
        flush=True,
    )

    return "generator", generator_id, pid, generation_no, str(path)


def file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--generators", type=int, default=1)
    parser.add_argument("--gets-per-client", type=int, default=12)
    parser.add_argument("--generations", type=int, default=6)
    parser.add_argument("--n-rows", type=int, default=1 << 18)
    parser.add_argument("--cols", type=int, default=20)
    parser.add_argument("--n-int-cols", type=int, default=4)
    parser.add_argument("--n-str-cols", type=int, default=8)
    parser.add_argument("--data-dir", type=Path, default=Path("parquet"))
    parser.add_argument("--cache-dir", type=Path, default=Path("__cache__/swarm_nfs"))
    args = parser.parse_args()

    if args.clients < 1:
        parser.error("--clients must be >= 1")
    if args.generators < 1:
        parser.error("--generators must be >= 1")
    if args.gets_per_client < 1:
        parser.error("--gets-per-client must be >= 1")
    if args.generations < 1:
        parser.error("--generations must be >= 1")

    path = generate_parquet(
        data_dir=args.data_dir,
        n_rows=args.n_rows,
        n_cols=args.cols,
        n_int_cols=args.n_int_cols,
        n_str_cols=args.n_str_cols,
    )

    max_workers = args.clients + args.generators
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        generation_steps = generation_wave_steps(
            gets_per_client=args.gets_per_client,
            generations=args.generations,
        )
        generation_no = 0
        for get_no in range(1, args.gets_per_client + 1):
            futures: list[Future[tuple[object, ...]]] = [
                executor.submit(get_once, client_id, path, args.cache_dir)
                for client_id in range(1, args.clients + 1)
            ]

            if get_no in generation_steps:
                for generator_id in range(1, args.generators + 1):
                    generation_no += 1
                    futures.append(
                        executor.submit(
                            generate_once,
                            generator_id,
                            generation_no,
                            data_dir=args.data_dir,
                            n_rows=args.n_rows,
                            n_cols=args.cols,
                            n_int_cols=args.n_int_cols,
                            n_str_cols=args.n_str_cols,
                            generation_total=args.generations * args.generators,
                        )
                    )

            for future in futures:
                result = future.result()
                print(
                    f"[swarm] wave={get_no}/{args.gets_per_client} result={result}",
                    flush=True,
                )

    run_get_round(
        clients=args.clients,
        path=path,
        cache_dir=args.cache_dir,
    )

    return 0


def generation_wave_steps(*, gets_per_client: int, generations: int) -> set[int]:
    return {
        max(1, min(gets_per_client, round((i * gets_per_client) / generations)))
        for i in range(1, generations + 1)
    }


def run_get_round(
    *,
    clients: int,
    path: Path,
    cache_dir: Path,
) -> None:
    print(f"[swarm] Final warm check: {clients} clients", flush=True)
    with ProcessPoolExecutor(max_workers=clients) as executor:
        futures = [
            executor.submit(get_once, client_id, path, cache_dir)
            for client_id in range(1, clients + 1)
        ]
        results = [future.result() for future in futures]
    summary = ", ".join(
        f"client={client_id} pid={pid} rows={rows} cols={cols}"
        for client_id, pid, rows, cols in results
    )
    print(f"[swarm] Final warm check done: {summary}", flush=True)

    return None


if __name__ == "__main__":
    raise SystemExit(main())
