import os
import polars as po
from polars.datatypes import Int64, Utf8
import ramda as R
from .download import download

URL = "http://static-files/thryve/exportStudy.7z"


def test_download_returns_list_of_dataframes():
    downloads = download(
        {
            "username": os.environ["THRYVE_FTP_USER"],
            "password": os.environ["THRYVE_FTP_PASSWORD"],
            "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
        },
        URL,
    )
    assert isinstance(downloads, dict)
    for key, value in downloads.items():
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
    assert R.pipe(R.prop("questionnaires"), lambda df: df.dtypes, R.tap(print))(
        downloads
    ) == [Int64, Utf8, Utf8, Int64, Int64]
    assert R.pipe(R.prop("answers"), lambda df: df.dtypes, R.tap(print))(downloads) == [
        Int64
    ] * 9 + [Utf8]
