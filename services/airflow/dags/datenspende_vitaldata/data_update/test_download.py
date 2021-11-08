import os

import polars as po
from polars.datatypes import Int64, Utf8, Date64
import ramda as R
from .download import download, DataList

URL = "http://static-files/thryve/export.7z"


def test_download_returns_list_of_dataframes():
    downloads = download(
        {
            "username": os.environ["THRYVE_FTP_USER"],
            "password": os.environ["THRYVE_FTP_PASSWORD"],
            "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
        },
        URL,
    )
    assert isinstance(downloads, list)
    for key, value in downloads:
        assert isinstance(key, str)
        assert isinstance(value, po.DataFrame)


def test_download_returns_empty_columns_with_correct_type():
    downloads = download(
        {
            "username": os.environ["THRYVE_FTP_USER"],
            "password": os.environ["THRYVE_FTP_PASSWORD"],
            "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
        },
        URL,
    )
    assert R.pipe(
        get_key_from_data_list("vitaldata"), lambda df: df.dtypes, R.tap(print)
    )(downloads) == [Int64, Date64, Int64, Int64, Int64, Int64, Int64]
    # assert R.pipe(
    #     get_key_from_data_list("users"), lambda df: df.dtypes, R.tap(print)
    # )(downloads) == [Int64, Utf8, Int64, Int64, Int64, Int64, Int64, Int64]


@R.curry
def get_key_from_data_list(key: str, data_list: DataList) -> po.DataFrame:
    for list_key, value in data_list:
        if list_key == key:
            return value
