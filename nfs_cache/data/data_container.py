from typing import Any

import polars as pl

from nfs_cache.data.data_holder import DataHolder


class DataContainer:
    __slots__ = ("data",)

    def __init__(
            self,
            input_data: dict[str, Any],
    ) -> None:
        self.data = DataHolder()
        self.data.headers = tuple(input_data["headers"])
        self.data.rows_data_pl = input_data["data"]


if __name__ == "__main__":
    headers = ("COL1", "COL2", "COL3")
    rows = [(1.1, 1, "A"), (2.2, 2, "B"), (3.3, 3, "C"), (4.4, 4, "D")]
    INPUT_DATA: dict[str, Any] = {
        "headers": headers,
        "data": pl.DataFrame(rows, schema=headers, orient="row"),
    }
    data = DataContainer(INPUT_DATA)
    print("headers:", data.data.headers)
    print("table:", data.data.rows_data_pl)
