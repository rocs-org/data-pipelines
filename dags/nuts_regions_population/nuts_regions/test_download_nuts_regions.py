import pandas as pd

from .download_nuts_regions import download_nuts_regions

URL = "http://static-files/static/NUTS2021.xlsx"


def test_download_nuts_regions_returns_dataframe_with_raw_data():
    regions_data = download_nuts_regions(URL)
    assert type(regions_data) == pd.DataFrame
    assert "Country" in regions_data.columns
    assert "Code 2021" in regions_data.columns
    assert "NUTS level" in regions_data.columns
    assert "NUTS level 1" in regions_data.columns
    assert "NUTS level 2" in regions_data.columns
    assert "NUTS level 3" in regions_data.columns
