import polars
import polars as po
import glob
from polars.datatypes import Utf8, Int64
import ramda as R
from typing import List, Tuple

from dags.helpers.dag_helpers import download_7zfile, unzip_7zfile


DataList = List[Tuple[str, List, polars.DataFrame]]


@R.curry
def download(access_config: dict, url: str) -> DataList:
    return R.pipe(
        download_7zfile(access_config),
        unzip_7zfile(access_config),
        load_files,
        map_dict_to_list,
    )(url)


def load_files(*_):
    vitaldata = {
        file: {
            "table": "vitaldata",
            "constraint": ["user_id", "date", "type", "source"],
            "df": po.read_csv(
                file,
                dtype={
                    "customer": Int64,
                    "day": Utf8,
                    "type": Int64,
                    "longValue": Int64,
                    "source": Int64,
                    "createdAt": Int64,
                    "timezoneOffset": Int64,
                },
                parse_dates=False,
            ),
        }
        for file in glob.glob("./dailies*.csv")
    }

    usersdata = {
        file: {
            "table": "users",
            "constraint": ["user_id"],
            "df": po.read_csv(
                file,
                dtype={
                    "customer": Int64,
                    "plz": Utf8,
                    "salutation": Int64,
                    "birthDate": Int64,
                    "weight": Int64,
                    "height": Int64,
                    "creationTimestamp": Int64,
                    "source": Int64,
                },
            ),
        }
        for file in ["./usersAll.csv"]
    }
    return {**usersdata, **vitaldata}


def map_dict_to_list(d: dict) -> list:
    return [(d[key]["table"], d[key]["constraint"], d[key]["df"]) for key in d.keys()]
