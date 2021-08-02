from dags.database.db_context import (
    DB_Context,
    query_all_elements,
)
from airflow.models import DagBag

try:
    from dags.test_helpers import execute_dag
except ModuleNotFoundError:
    print("Absolute imports failed")
    from test_helpers import execute_dag  # type: ignore


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("csv_download_to_postgres.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_with_no_errors(db_context: DB_Context):
    credentials = db_context["credentials"]

    assert (
        execute_dag(
            "example_csv_to_postgres",
            "2021-01-01",
            {"TARGET_DB": credentials["database"]},
        )
        == 0
    )
    res = query_all_elements(db_context, "SELECT * FROM test_table")

    assert len(res) == 2
