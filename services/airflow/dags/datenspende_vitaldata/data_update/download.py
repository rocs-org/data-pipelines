import polars
import requests
import py7zr
import polars as po
import glob
from polars.datatypes import Utf8, Int64
import ramda as R
from typing import List, Tuple
from returns.curry import curry
from requests.auth import HTTPBasicAuth


DataList = List[Tuple[str, polars.DataFrame]]


@R.curry
def download(access_config: dict, url: str) -> DataList:
    return R.pipe(
        download_file(access_config),
        unzip_file(access_config),
        load_files,
        map_dict_to_list,
    )(url)


@curry
def download_file(access_config: dict, url):
    local_filename = "export.7z"
    with requests.get(
        url,
        stream=True,
        auth=HTTPBasicAuth(access_config["username"], access_config["password"]),
    ) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


@curry
def unzip_file(access_config: dict, filename: str) -> None:
    with py7zr.SevenZipFile(
        filename, "r", password=access_config["zip_password"]
    ) as file:
        file.extractall()


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
