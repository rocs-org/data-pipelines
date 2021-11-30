import io

import ramda as R
import requests
import py7zr
from returns.curry import curry
from requests.auth import HTTPBasicAuth
from pandas import DataFrame, read_csv


@curry
def download_7zfile(access_config: dict, url):
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
def unzip_7zfile(access_config: dict, filename: str) -> None:
    with py7zr.SevenZipFile(
        filename, "r", password=access_config["zip_password"]
    ) as file:
        file.extractall()


def download_csv(url: str) -> DataFrame:
    return R.pipe(
        R.try_catch(
            requests.get,
            lambda err, url: _raise(
                FileNotFoundError(f"Cannot find file at {url} \n trace: \n {err}")
            ),
        ),
        R.prop("content"),
        R.invoker(1, "decode")("utf-8"),
        io.StringIO,
        read_csv,
    )(url)


def _raise(ex: Exception):
    raise ex
