from typing import Any
import polars as pl

from nfs_cache.data.data_holder import DataHolder

class DataContainer:
    __slots__ = ("data",)

    def __init__(
        self,
        input_data: dict[str, tuple | tuple[Any, ...] | pl.DataFrame],
    ) -> None:
        self.data = DataHolder()
        self.data.headers = tuple(input_data["headers"])
        self.data.meta_data = input_data.get("meta-data", None)

        # store the actual table data, depending on orientation
        self.data.rows_data_pl = input_data["data"]

if __name__ == "__main__":
    INPUT_DATA: dict[str, Any] = {
        "headers": ("COL1", "COL2", "COL3"),
        "data": [(1.1, 1, "A"), (2.2, 2, "B"), (3.3, 3, "C"), (4.4, 4, "D")],
    }
    data = DataContainer()
    data.headers = tuple(INPUT_DATA["headers"])
    data.rows_data_pl = pl.DataFrame(INPUT_DATA["data"], schema=INPUT_DATA["headers"], orient="row")
    print("headers:", data.headers)
    print("table:", data.rows_data_pl)