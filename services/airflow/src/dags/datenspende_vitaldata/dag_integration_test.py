from datetime import date

import pandas as pd
from airflow.models import DagBag
from pytest import approx

from postgres_helpers import DBContext, query_all_elements
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    connect_to_db_and_insert_pandas_dataframe,
)
from src.lib.test_helpers import execute_dag


def test_datenspende_vitals_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_vitals_dag_writes_correct_results_to_db(pg_context: DBContext):
    credentials = pg_context["credentials"]

    # inser user into exclusion criteria
    connect_to_db_and_insert_pandas_dataframe(
        "datenspende_derivatives",
        "excluded_users",
        pd.DataFrame(
            {
                "user_id": [400],
                "project": ["scripps colaboration long covid"],
                "reason": ["outlier"],
            }
        ),
    )

    # insert infection records into unified features
    connect_to_db_and_insert_pandas_dataframe(
        "datenspende_derivatives",
        "homogenized_features",
        pd.DataFrame(
            {
                "user_id": [100, 200, 300, 400],
                "test_week_start": 4 * [date(year=2022, month=1, day=1)],
                "f10": 2 * [True] + 2 * [False],
            }
        ),
    )

    assert (
        execute_dag(
            "datenspende_vitaldata_v2",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )
    vitals_from_db = query_all_elements(
        pg_context, "SELECT * FROM datenspende.vitaldata;"
    )
    print(vitals_from_db[-1])
    assert vitals_from_db[-1] == (
        400,
        date(2021, 10, 28),
        9,
        4401,
        6,
        1635284920437,
        120,
    )

    assert len(vitals_from_db) == 67

    vitals_from_db = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.steps_ct;"
    )
    assert len(vitals_from_db) == 4

    aggregates = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.daily_aggregates_of_vitals ORDER BY type, date;",
        pg_context,
    )

    # Aggregates of 4 types of vitals on 5 days
    assert len(aggregates) == 4 * 5

    standardized_vitals = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.vitals_standardized_by_daily_aggregates ORDER BY user_id, type, date;",
        pg_context,
    )

    # 400 is excluded
    assert 400 not in list(standardized_vitals["user_id"].values)

    # values are standardized or have mean subtracted
    df = standardized_vitals.groupby(["date", "type", "source"]).agg(
        {"value_minus_mean": "mean", "standardized_value": ["mean", "std"]}
    )
    df.columns = ["mean1", "mean2", "std"]

    for value in df["mean1"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["mean2"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["std"].values:
        assert value == approx(1, abs=TOLERANCE)

    agg_from_std = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.agg_before_infection_from_vitals_std_by_day ORDER BY type, user_id;",
        pg_context,
    )

    # Aggregates of 4 types of vitals on 5 days minus some, where standardization with std=0 was skipped
    assert len(agg_from_std) == 17

    standardized_vitals_2 = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.vitals_std_by_date_and_user_before_infection ORDER BY user_id, date;",
        pg_context,
    )

    # 400 is excluded
    assert 400 not in list(standardized_vitals_2["user_id"].values)

    print(standardized_vitals_2.columns)
    # values are standardized or have mean subtracted
    df = standardized_vitals_2.groupby(["user_id", "type", "source"]).agg(
        {
            "value_minus_mean_from_standardized": "mean",
            "value_minus_mean_from_value_minus_mean": "mean",
            "standardized_value_from_value_minus_mean": ["mean", "std"],
            "standardized_value": ["mean", "std"],
        }
    )

    print(df)
    print(df.columns)

    df.columns = [
        "mean0",
        "mean1",
        "mean2",
        "std1",
        "mean3",
        "std2",
    ]

    print(df[["std1", "std2"]])

    for value in df["mean0"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["mean1"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["mean2"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["mean3"].values:
        assert value == approx(0, abs=TOLERANCE)
    for value in df["std1"].values:
        assert value == approx(1, abs=TOLERANCE)
    for value in df["std2"].values:
        assert value == approx(1, abs=TOLERANCE)

    user_vital_stats = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.user_vital_stats;"
    )
    assert len(user_vital_stats) == 4

    vitaldata_per_day = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.vitaldata_per_day;"
    )
    assert len(vitaldata_per_day) == 20

    user_survey_stats = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.user_survey_stats;"
    )
    assert len(user_survey_stats) == 4

    surveys_per_day = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.surveys_per_day;"
    )
    assert len(surveys_per_day) == 20


TOLERANCE = 1e-8
THRYVE_FTP_URL = "http://static-files/thryve/export_scripps.7z"
