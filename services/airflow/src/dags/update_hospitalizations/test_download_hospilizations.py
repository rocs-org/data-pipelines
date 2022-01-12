import pandas.api.types as ptypes

from src.dags.update_hospitalizations.download_hospitalizations import (
    download_hospitalizations,
)

url = "http://static-files/static/hospitalizations.csv"


def test_download_hospitalizations_col_names_match():
    hospitalizations = download_hospitalizations(url)
    assert list(hospitalizations.columns) == [
        "country",
        "indicator",
        "date",
        "year_week",
        "value",
        "source",
        "url",
    ]


def test_download_hopsitalizations_dtypes_match():
    hospitalizations = download_hospitalizations(url)
    assert ptypes.is_string_dtype(hospitalizations.country)
    assert ptypes.is_string_dtype(hospitalizations.indicator)
    assert ptypes.is_string_dtype(hospitalizations.date)
    assert ptypes.is_string_dtype(hospitalizations.year_week)
    assert ptypes.is_string_dtype(hospitalizations.source)
    assert ptypes.is_string_dtype(hospitalizations.url)
    assert ptypes.is_float_dtype(hospitalizations.value)
