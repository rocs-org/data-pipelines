import glob

import polars as po
from polars.datatypes import Utf8, Int64

from src.lib.dag_helpers.download_helpers import extract


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
        for file in sorted(glob.glob("./dailies*.csv"))
    }

    usersdata = {
        "./usersAll.csv": {
            "table": "users",
            "constraint": ["user_id"],
            "df": po.read_csv(
                "usersAll.csv",
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
    }
    return {**usersdata, **vitaldata}


def map_dict_to_list(d: dict) -> list:
    return [(d[key]["table"], d[key]["constraint"], d[key]["df"]) for key in d.keys()]


download = extract(load_files, map_dict_to_list)
