import os
import pandas as pd
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
        assert isinstance(value, pd.DataFrame)
