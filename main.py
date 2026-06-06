from pathlib import Path

from nfs_cache.data.data_container import DataContainer
from nfs_cache.db_cache import DBCache

import polars as pl

dbcache = DBCache(Path("__cache__/nfs"))


@dbcache.data_container_cache
def load_data_container(path: Path) -> DataContainer:
    print(f"Reading: {path}...")
    df = pl.read_parquet(path)
    data = DataContainer({"headers": tuple(df.columns), "data": df}) # can also be from Oracle, MySQL, etc.
    return data

def main():
    # cache cold, execute: load_data_container
    load_data_container(Path("parquet/A_TEST_1048576.parquet"))

    # cache warm, get DataContainer from NFS cache
    load_data_container(Path("parquet/A_TEST_1048576.parquet"))


if __name__ == "__main__":
    main()
