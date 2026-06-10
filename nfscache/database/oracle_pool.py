"""Process-local oracledb connection pool, exposed as an NFSParquetCache `connect_factory`.

`NFSParquetCache.connect_factory` is an opaque `Callable[[], connection]` that the cache
uses as `with connect_factory() as conn:`. A pooled connection's `__exit__`
*releases* it back to the pool instead of closing the socket, so wiring a pool in
here removes the per-call `oracledb.connect` cost on every version probe and warm
hit while keeping `nfs_parquet_cache.py` free of any oracledb dependency.

Pools are cached per process (keyed by pid + DSN + user): connections are not
shareable across processes (ProcessPoolExecutor workers each build their own), and
a forked child must not reuse the parent's pool. The pid in the key guards that.
"""

import argparse
import os
import threading

import oracledb

_pools: dict[tuple[int, str, str], "oracledb.ConnectionPool"] = {}
_lock = threading.Lock()


def _dsn(args: argparse.Namespace) -> str:
    return f"{args.host}:{args.port}/{args.service}"


def get_pool(
    args: argparse.Namespace,
    *,
    min_size: int = 1,
    max_size: int = 4,
) -> "oracledb.ConnectionPool":
    """Return a process-local pool for `args`, creating it once per process."""
    key = (os.getpid(), _dsn(args), args.user)
    pool = _pools.get(key)
    if pool is not None:
        return pool

    with _lock:
        pool = _pools.get(key)
        if pool is None:
            pool = oracledb.create_pool(
                user=args.user,
                password=args.password,
                dsn=_dsn(args),
                min=min_size,
                max=max_size,
            )
            _pools[key] = pool
        return pool


def make_pool_factory(
    args: argparse.Namespace,
    *,
    min_size: int = 1,
    max_size: int = 4,
):
    """Build a `connect_factory` that acquires from a process-local pool.

    The pool is created lazily on first call, so building the factory never
    touches the database (safe to do at import time).
    """

    def factory() -> "oracledb.Connection":
        return get_pool(args, min_size=min_size, max_size=max_size).acquire()

    return factory
