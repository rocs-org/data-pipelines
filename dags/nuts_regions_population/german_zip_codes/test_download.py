import pandas as pd
from pandas.api.types import is_string_dtype

from .download import download

URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"


def test_download_zip_codes_returns_dataframe():
    zip_codes = download(URL)
    assert isinstance(zip_codes, pd.DataFrame)


def test_download_zip_codes_df_has_expected_columns_and_types():
    zip_codes = download(URL)
    assert list(zip_codes.columns) == ["NUTS3", "CODE"]
    assert is_string_dtype(zip_codes["NUTS3"])
    assert is_string_dtype(zip_codes["CODE"])
