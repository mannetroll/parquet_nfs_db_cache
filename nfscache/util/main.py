import shutil
from pathlib import Path

import polars as pl

from nfscache.data.data_container import DataContainer
from nfscache.nfs_cache import NFSCache
from nfscache.util.generate_parquets import ensure_one_parquet

CACHE_ROOT = Path("__cache__")
nfscache = NFSCache(CACHE_ROOT / "nfs")
DATA_DIR = Path("parquet")
N_ROWS = 1_048_576
DATA_PATH = DATA_DIR / f"A_TEST_{N_ROWS}.parquet"


@nfscache.parquet
def load_data_container(filename: Path) -> DataContainer:
    print(f"Reading: {filename}...")
    df = pl.read_parquet(filename)
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


def clear_cache_root() -> None:
    if not CACHE_ROOT.exists():
        return

    if not CACHE_ROOT.is_dir():
        print(f"Clearing cache file: {CACHE_ROOT}")
        CACHE_ROOT.unlink()
        return

    print(f"Clearing cache: {CACHE_ROOT}")
    for path in CACHE_ROOT.iterdir():
        if path.name == ".gitignore":
            continue
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()


def main():
    clear_cache_root()

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
