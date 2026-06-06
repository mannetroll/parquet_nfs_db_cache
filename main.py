from pathlib import Path

from nfs_cache.data.data_container import DataContainer

import polars as pl

def load_data_container(path: Path) -> DataContainer:
    df = pl.read_parquet(path)
    data = DataContainer({"headers": tuple(df.columns), "data": df}) # can also be from Oracle, MySQL, etc.
    return data

def main():
    container = load_data_container(Path("parquet/A_TEST_1048576.parquet"))
    print(container.data.rows_data_pl)

if __name__ == "__main__":
    main()