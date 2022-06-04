from typing import List

import pandas as pd
import ramda as R
from pandas.core import groupby
from datetime import date, timedelta, datetime

from .shared import post_processing_vitals_pipeline_factory

DB_PARAMETERS = [
    "datenspende_derivatives",
    "daily_vital_rolling_window_time_series_features",
    ["one_value_per_user_day_and_type"],
]

USER_BATCH_SIZE = 20000
LOAD_LAST_N_DAYS = 57


@R.curry
def rolling_window_statistics_of_user_vitals(
    execution_date: date,
    user_vital_data: pd.DataFrame,
) -> pd.DataFrame:
    return R.pipe(
        cast_column_to_date("date"),
        R.converge(
            merge_dataframes,
            [
                calculate_8_week_statistics(execution_date),
                calculate_one_week_statistics(execution_date),
            ],
        ),
    )(user_vital_data)


@R.curry
def group_df_by_columns(
    columns: List[str], df: pd.DataFrame
) -> groupby.generic.DataFrameGroupBy:
    return df.groupby(columns)


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df1, df2, how="outer", on=["user_id", "type", "source", "date"])


@R.curry
def calculate_8_week_statistics(execution_date: date, df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.query(
            f"date > {datetime.strftime(execution_date - timedelta(days=57), '%Y%m%d')}"
        )
        .groupby(["user_id", "type", "source"])
        .rolling("56D", min_periods=30, on="date")
        .agg({"value": ["mean", "min", "max", "median"]})
        .droplevel(level=0, axis="columns")
        .reset_index()
        .rename(
            columns={
                "mean": "fiftysix_day_mean_min_30_values",
                "min": "fiftysix_day_min_min_30_values",
                "max": "fiftysix_day_max_min_30_values",
                "median": "fiftysix_day_median_min_30_values",
            }
        )
    )


@R.curry
def calculate_one_week_statistics(
    execution_date: date, df: pd.DataFrame
) -> pd.DataFrame:
    return (
        df.query(
            f"date > {datetime.strftime(execution_date - timedelta(days=8), '%Y%m%d')}"
        )
        .groupby(["user_id", "type", "source"])
        .rolling("7D", min_periods=3, on="date")
        .agg({"value": ["mean", "min", "max", "median"]})
        .droplevel(level=0, axis="columns")
        .reset_index()
        .rename(
            columns={
                "mean": "seven_day_mean_min_3_values",
                "max": "seven_day_max_min_3_values",
                "min": "seven_day_min_min_3_values",
                "median": "seven_day_median_min_3_values",
            }
        )
    )


@R.curry
def cast_column_to_date(column: str, df: pd.DataFrame) -> pd.DataFrame:
    df[column] = df[column].apply(pd.to_datetime)
    return df.sort_values("date")


rolling_window_time_series_features_pipeline = post_processing_vitals_pipeline_factory(
    rolling_window_statistics_of_user_vitals,
    DB_PARAMETERS,
    USER_BATCH_SIZE,
    LOAD_LAST_N_DAYS,
)
