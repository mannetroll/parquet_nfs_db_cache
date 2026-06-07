from typing import Any

import polars as pl

class DataHolder:
    headers: tuple
    rows_data_pl: pl.DataFrame | None = None

    def __init__(self):
        self.headers = tuple()
        self.rows_data_pl = pl.DataFrame()


if __name__ == "__main__":
    INPUT_DATA: dict[str, Any] = {
        "headers": ("COL1", "COL2", "COL3"),
        "data": [(1.1, 1, "A"), (2.2, 2, "B"), (3.3, 3, "C"), (4.4, 4, "D")],
    }
    data = DataHolder()
    data.headers = tuple(INPUT_DATA["headers"])
    data.rows_data_pl = pl.DataFrame(INPUT_DATA["data"], schema=INPUT_DATA["headers"], orient="row")
    print("headers:", data.headers)
    print("table:", data.rows_data_pl)
