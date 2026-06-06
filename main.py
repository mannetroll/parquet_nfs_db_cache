from pathlib import Path

from nfs_cache.data.data_container import DataContainer

import polars as pl

# dbcache = DBCache(Path("/nfs/cache/parquet"),")

#@dbcache.data_container_cache
def load_data_container(path: Path) -> DataContainer:
    print(f"Reading: {path}...")
    df = pl.read_parquet(path)
    data = DataContainer({"headers": tuple(df.columns), "data": df}) # can also be from Oracle, MySQL, etc.
    return data

def main():
    # cache cold, execute: load_data_container
    call1 = load_data_container(Path("parquet/A_TEST_1048576.parquet"))
    print(call1.data.rows_data_pl)

    # cache warm, get DataContainer from NFS cache
    call2 = load_data_container(Path("parquet/A_TEST_1048576.parquet"))
    print(call2.data.rows_data_pl)


if __name__ == "__main__":
    main()
