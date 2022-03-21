import datetime

from psycopg2.sql import SQL

from postgres_helpers import DBContext
from src.dags.datenspende_vitaldata.post_processing.test_pivot_tables import (
    setup_vitaldata_in_db,
)
from src.lib.dag_helpers import execute_query_and_return_dataframe
from .time_series_features import rolling_window_time_series_features_pipeline


def test_time_series_features_calculated_rolling_window_aggregates(
    pg_context: DBContext,
):
    setup_vitaldata_in_db("http://static-files/thryve/export_time_series.7z")
    rolling_window_time_series_features_pipeline()

    last_row_of_user_100 = list(
        execute_query_and_return_dataframe(
            SQL(
                """
                SELECT * FROM
                datenspende_derivatives.daily_vital_rolling_window_time_series_features
                WHERE user_id=100;
            """
            ),
            pg_context,
        )
        .replace({float("nan"): -1})
        .values[-1]
    )
    print(last_row_of_user_100)

    assert last_row_of_user_100 == [
        100,
        65,
        3,
        datetime.date(2021, 10, 25),
        -1.0,
        -1.0,
        -1.0,
        -1.0,
        50.0,
        50.0,
        50.0,
        50.0,
    ]

    last_row_of_user_236 = list(
        execute_query_and_return_dataframe(
            SQL(
                """
                SELECT *
                FROM datenspende_derivatives.daily_vital_rolling_window_time_series_features
                WHERE user_id=236;
            """
            ),
            pg_context,
        )
        .replace({float("nan"): -1})
        .values[-1]
    )
    print(last_row_of_user_236)
    assert last_row_of_user_236 == [
        236,
        65,
        6,
        datetime.date(2022, 3, 19),
        50.25,
        41.0,
        63.0,
        49.5,
        53.0,
        49.0,
        61.0,
        51.0,
    ]
