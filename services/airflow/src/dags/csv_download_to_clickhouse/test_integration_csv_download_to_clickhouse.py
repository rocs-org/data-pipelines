import pandas as pd
from airflow.models import DagBag

from clickhouse_helpers import DBContext, query_dataframe
from src.lib.test_helpers import execute_dag

URL = "http://static-files/static/test_ch.csv"


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("csv_download_to_postgres.py")
    assert len(dag_bag.import_errors) == 0


def test_clickhouse_dag_executes_with_no_errors(ch_context: DBContext):
    credentials = ch_context["credentials"]

    assert (
        execute_dag(
            "extract_load_to_clickhouse",
            "2021-01-01",
            {"CLICKHOUSE_DB": credentials["database"], "URL": URL},
        )
        == 0
    )

    res = query_dataframe(ch_context, "SELECT col1, col2, col3 FROM test_table")

    assert len(res) == 2
    assert (
        res.values
        == pd.DataFrame(
            {"col1": [1, 2], "col2": ["hello", "not"], "col3": ["world", "today"]}
        ).values
    ).all()
