from .transform import transform
from .download import download
import ramda as R

URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"


def test_transform_returns_correct_columns():
    transformed_zips = R.pipe(download, transform)(URL)

    assert list(transformed_zips.columns) == ["nuts", "zip_code"]


def test_transform_removes_single_quotes_from_strings():
    transformed_zips = R.pipe(download, transform)(URL)

    assert "'" not in transformed_zips.iloc[0][0]
