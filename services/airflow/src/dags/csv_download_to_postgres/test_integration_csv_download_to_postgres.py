from database import DBContext, query_all_elements
from airflow.models import DagBag
from src.lib.test_helpers import execute_dag

URL = "http://static-files/static/test.csv"


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("csv_download_to_postgres.py")
    assert len(dag_bag.import_errors) == 0


def test_postgres_dag_executes_with_no_errors(pg_context: DBContext):
    credentials = pg_context["credentials"]

    assert (
        execute_dag(
            "example_csv_to_postgres",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": URL},
        )
        == 0
    )

    res = query_all_elements(pg_context, "SELECT * FROM test_tables.test_table")

    assert len(res) == 2
