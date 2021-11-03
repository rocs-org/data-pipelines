import os

import polars as po
from .download import download

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


# TODO: the loading test of the *.csv with correct types
