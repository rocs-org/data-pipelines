import glob
from typing import List, Tuple

import pandas as pd
from src.lib.dag_helpers.download_helpers import extract as extraction_factory

DataList = List[Tuple[str, pd.DataFrame]]


def load_files(*_):
    return {
        file: pd.read_csv(
            file,
        ).replace({float("nan"): None})
        for file in sorted(glob.glob("./epoch*.csv"))
    }


def map_dict_to_list(d: dict) -> list:
    return [(filename, data) for filename, data in d.items()]


extract = extraction_factory(load_files, map_dict_to_list)
