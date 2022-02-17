import glob
from typing import List, Tuple

import pandas as pd
import ramda as R

from src.lib.dag_helpers import download_7zfile, unzip_7zfile

DataList = List[Tuple[str, pd.DataFrame]]


@R.curry
def extract(access_config: dict, url: str) -> DataList:
    return R.pipe(
        download_7zfile(access_config),
        unzip_7zfile(access_config),
        load_files,
        map_dict_to_list,
    )(url)


def load_files(*_):
    return {
        file: pd.read_csv(
            file,
        ).replace({float("nan"): None})
        for file in sorted(glob.glob("./epoch*.csv"))
    }


def map_dict_to_list(d: dict) -> list:
    return [(filename, data) for filename, data in d.items()]
