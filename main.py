from pathlib import Path

from nfs_cache.data.data_container import DataContainer
from nfs_cache.db_cache import DBCache
from nfs_cache.util.generate_parquets import ensure_one_parquet

import polars as pl

dbcache = DBCache(Path("__cache__/nfs"))
DATA_DIR = Path("parquet")
N_ROWS = 1_048_576
DATA_PATH = DATA_DIR / f"A_TEST_{N_ROWS}.parquet"


@dbcache.data_container_cache
def load_data_container(path: Path) -> DataContainer:
    print(f"Reading: {path}...")
    df = pl.read_parquet(path)
    data = DataContainer({"headers": tuple(df.columns), "data": df}) # can also be from Oracle, MySQL, etc.
    return data


def generate_parquet() -> Path:
    print(f"Generating: {DATA_PATH}...")
    return ensure_one_parquet(
        out_dir=DATA_DIR,
        base_name=f"TEST_{N_ROWS}.parquet",
        prefix="A_",
        n_rows=N_ROWS,
        n_cols=20,
        seed=None,
        float_scale=5.0,
        n_int_cols=4,
        n_str_cols=8,
    )


def main():
    path = generate_parquet()

    # cache cold, execute: load_data_container
    load_data_container(path)

    # cache warm, get DataContainer from NFS cache
    load_data_container(path)

    path = generate_parquet()

    # source changed, reload and replace stale cache entry
    load_data_container(path)

    # cache warm again after reload
    load_data_container(path)


if __name__ == "__main__":
    main()
