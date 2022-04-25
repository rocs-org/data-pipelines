from postgres_helpers import DBContext, query_all_elements
from airflow.models import DagBag
from datetime import date
import pandas as pd
from src.lib.dag_helpers import execute_query_and_return_dataframe

from src.dags.datenspende_vitaldata.post_processing.shared_test import (
    SECOND_TEST_USER_DATA,
)

from src.lib.test_helpers import execute_dag


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_vitals_dag_writes_correct_results_to_db(pg_context: DBContext):
    credentials = pg_context["credentials"]

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
        200,
        date(2021, 10, 28),
        9,
        4601,
        6,
        1635284920437,
        120,
    )
    assert len(vitals_from_db) == 35

    vitals_from_db = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.steps_ct;"
    )
    assert len(vitals_from_db) == 2

    aggregate_statistics = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.daily_vital_statistics;", pg_context
    )
    assert (
        aggregate_statistics.query("user_id == 100 and type == 9 and source == 6")[
            "std"
        ].values[0]
        == 0
    )
    assert (
        aggregate_statistics.query("user_id == 100 and type == 9 and source == 6")[
            "mean"
        ].values[0]
        == pd.DataFrame(SECOND_TEST_USER_DATA)
        .query("user_id == 100 and type == 9 and source == 6")["value"]
        .mean()
    )

    aggregates = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.aggregates_for_standardization_by_type_source_date;",
        pg_context,
    )

    print(aggregates)

    standardized_vitals = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.vitals_standardized_by_daily_aggregates;",
        pg_context,
    )

    print(standardized_vitals)

    assert False


THRYVE_FTP_URL = "http://static-files/thryve/export.7z"
