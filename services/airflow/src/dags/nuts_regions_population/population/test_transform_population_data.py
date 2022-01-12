import pandas as pd
import ramda as R
from database import DBContext
from src.lib.dag_helpers import (
    connect_to_db_and_insert_pandas_dataframe,
)
from .transform_population_data import (
    transform,
    INDICATORS,
    set_nth_indicator_as_column,
    split_number_into_int_and_data_flag,
)
from .download_population_data import download_data
import pandas.api.types as ptypes

URL = "http://static-files/static/demo_r_pjangrp3.tsv"


def test_transform_returns_correct_columns(db_context: DBContext):
    insert_some_nuts_regions_into_db_to_filter_agains()
    df = R.pipe(download_data, transform)(URL)

    assert set(list(df.columns)) == {
        "year",
        "number",
        "nuts",
        "agegroup",
        "sex",
        "data_quality_flags",
    }


def test_transform_population_returns_correct_dtypes(db_context: DBContext):
    insert_some_nuts_regions_into_db_to_filter_agains()

    transformed = R.pipe(download_data, transform)(URL)
    assert ptypes.is_integer_dtype(transformed["year"])
    assert ptypes.is_integer_dtype(transformed["number"])
    assert ptypes.is_string_dtype(transformed["nuts"])
    assert ptypes.is_string_dtype(transformed["agegroup"])
    assert ptypes.is_string_dtype(transformed["sex"])
    assert ptypes.is_string_dtype(transformed["data_quality_flags"])


def test_split_number_into_int_and_data_flag_splits_values_correctly():
    res = split_number_into_int_and_data_flag("123 b")
    assert res[0] == 123
    assert res[1] == "break in time series,"

    res = split_number_into_int_and_data_flag("123 be")
    assert res[1] == "break in time series,estimated,"


def test_split_number_into_ind_and_data_flag_defaults_to_empty_string():
    res = split_number_into_int_and_data_flag("1234")
    assert res[1] == ""


def test_split_number_into_ind_and_data_flag_defaults_also_works_on_ints():
    res = split_number_into_int_and_data_flag(1234)
    assert res[0] == 1234
    assert res[1] == ""


def test_set_sex_as_column():
    df = pd.DataFrame(
        data=[["F,NR,TOTAL,AL"], ["M,NR,TOTAL,AL"], ["T,NR,TOTAL,AL"]],
        columns=[INDICATORS],
    )
    res = set_nth_indicator_as_column("sex", 0, df)
    assert "sex" in list(res.columns)
    assert ["F", "M", "T"] == list(res["sex"])


def test_filtering_for_existing_regions_deletes_rows_without_regions(
    db_context: DBContext,
):
    connect_to_db_and_insert_pandas_dataframe(
        "censusdata",
        "nuts",
        pd.DataFrame(
            columns=["level", "geo", "name", "country_id"],
            data=[[0, "ITI42", "something", 0], [1, "ITI43", "something", 0]],
        ),
    )
    filtered = R.pipe(download_data, transform)(URL)
    assert set(filtered["nuts"].values) == {"ITI42", "ITI43"}


def test_use_with_with_lambda_functions_1():
    assert 2 == R.use_with(lambda x, y: x + y, [R.identity, R.identity])(1, 1)


def insert_some_nuts_regions_into_db_to_filter_agains():
    return connect_to_db_and_insert_pandas_dataframe(
        "censusdata",
        "nuts",
        pd.DataFrame(
            columns=["level", "geo", "name", "country_id"],
            data=[[0, "ITI42", "something", 0], [1, "ITI43", "something", 0]],
        ),
    )
