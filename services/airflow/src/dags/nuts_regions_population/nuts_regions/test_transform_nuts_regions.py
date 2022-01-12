from .download_nuts_regions import download_nuts_regions
from .transform_nuts_regions import transform_nuts_regions
import ramda as R
import pandas.api.types as ptypes

URL = "http://static-files/static/NUTS2021.xlsx"


def test_transform_regions_returns_correct_collumns():
    transformed = R.pipe(download_nuts_regions, transform_nuts_regions)(URL)
    assert (transformed.columns == ["geo", "level", "name", "country_id"]).all()


def test_transform_regions_returns_correct_dtypes():
    transformed = R.pipe(download_nuts_regions, transform_nuts_regions)(URL)
    assert ptypes.is_integer_dtype(transformed["level"])
    assert ptypes.is_integer_dtype(transformed["country_id"])
    assert ptypes.is_string_dtype(transformed["geo"])
    assert ptypes.is_string_dtype(transformed["name"])


def test_transform_regions_drops_nans():
    transformed = R.pipe(download_nuts_regions, transform_nuts_regions)(URL)
    assert len(transformed[transformed["level"].isnull()]) == 0
    assert len(transformed[transformed["country_id"].isnull()]) == 0


def test_transform_region_returns_values():
    transformed = R.pipe(download_nuts_regions, transform_nuts_regions)(URL)
    assert len(transformed) > 1000
