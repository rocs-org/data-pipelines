import datetime

from airflow.models import DagBag
from psycopg2.sql import SQL

from postgres_helpers import DBContext
from src.lib.dag_helpers import execute_query_and_return_dataframe
from src.lib.test_helpers import execute_dag


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_vitals_rolling_window_dag_writes_correct_results_to_db(
    pg_context: DBContext,
):
    credentials = pg_context["credentials"]

    assert (
        execute_dag(
            "datenspende_vitaldata_v2",
            "2021-10-25",
            {"TARGET_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )

    assert (
        execute_dag(
            "datenspende_vitaldata_rolling_window",
            "2021-10-25",
            {"TARGET_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )

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


THRYVE_FTP_URL = "http://static-files/thryve/export.7z"
