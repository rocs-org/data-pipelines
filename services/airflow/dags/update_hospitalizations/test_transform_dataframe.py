import pandas.api.types as ptypes

from transform_dataframe import transform_dataframe
from download_hospitalizations import download_hospitalizations
from dags.helpers.test_helpers import run_task_with_url

run_task_with_url(
    "nuts_regions_population",
    "load_nuts_regions",
    "http://static-files/static/NUTS2021.xlsx",
)
run_task_with_url(
    "nuts_regions_population",
    "load_population_for_nuts_regions",
    "http://static-files/static/demo_r_pjangrp3.tsv",
)
url = "http://static-files/static/hospitalizations.csv"
transformed_df_column_names = [
    "new_hospitalizations",
    "new_hospitalizations_per_100k",
    "year",
    "calendar_week",
    "year_week",
    "date_begin",
    "date_end",
    "days_since_jan1_begin",
    "days_since_jan1_end",
    "date_updated",
    ]

def test_transform_dataframe_columns_match():
    df_tranformed = transform_dataframe(download_hospitalizations(url))
    assert list(df_tranformed.columns) == transformed_df_column_names

def test_transform_dataframe_dtypes_match():
    df_transformed = transform_dataframe(download_hospitalizations(url))
    assert ptypes.is_integer_dtype(df_transformed.new_hospitalizations)
    assert ptypes.is_float_dtype(df_transformed.new_hospitalizations_per_100k)
    assert ptypes.is_integer_dtype(df_transformed.year)
    assert ptypes.is_integer_dtype(df_transformed.calendar_week)
    assert ptypes.is_string_dtype(df_transformed.year_week)
    assert ptypes.is_integer_dtype(df_transformed.days_since_jan1_begin)
    assert ptypes.is_integer_dtype(df_transformed.days_since_jan1_end)
    assert ptypes.is_datetime64_dtype(df_transformed.date_updated)

