import os

import pandas as pd

from .extract import extract

URL = "http://static-files/thryve/exportEpoch.7z"


def test_download_epoch_returns_list_of_dataframes():
    downloads = extract(
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
        assert isinstance(value, pd.DataFrame)


def test_extract_returns_dataframes_with_matching_columns():
    downloads = extract(
        {
            "username": os.environ["THRYVE_FTP_USER"],
            "password": os.environ["THRYVE_FTP_PASSWORD"],
            "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
        },
        URL,
    )
    assert list(downloads[0][1].columns) == [
        "customer",
        "type",
        "valueType",
        "doubleValue",
        "longValue",
        "booleanValue",
        "timezoneOffset",
        "startTimestamp",
        "endTimestamp",
        "createdAt",
        "source",
    ]
