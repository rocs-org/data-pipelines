import ramda as R
import pandas as pd
import pandas.api.types as ptypes
from .download import download
from .transform import transform

URL = "http://static-files/static/04-kreise.xlsx"


def test_transformed_data_does_not_contain_nans():
    transformed = R.pipe(download, transform)(URL)
    length = len(transformed)
    assert length == len(transformed[pd.notnull(transformed["nuts"])])


def test_transformed_data_has_correct_columns():
    transformed = R.pipe(download, transform)(URL)

    assert list(transformed.columns) == COLUMNS


def test_transformed_data_has_correct_types():
    transformed = R.pipe(download, transform)(URL)
    assert ptypes.is_integer_dtype(transformed["german_id"])
    assert ptypes.is_string_dtype(transformed["regional_identifier"])
    assert ptypes.is_string_dtype(transformed["municipality"])
    assert ptypes.is_string_dtype(transformed["nuts"])
    assert ptypes.is_float_dtype(transformed["area"])
    assert ptypes.is_integer_dtype(transformed["population"])
    assert ptypes.is_integer_dtype(transformed["f_population"])
    assert ptypes.is_integer_dtype(transformed["m_population"])
    assert ptypes.is_float_dtype(transformed["population_density"])
    assert ptypes.is_datetime64_dtype(transformed["year"])


COLUMNS = [
    "german_id",
    "regional_identifier",
    "municipality",
    "nuts",
    "area",
    "population",
    "m_population",
    "f_population",
    "population_density",
    "year",
]
