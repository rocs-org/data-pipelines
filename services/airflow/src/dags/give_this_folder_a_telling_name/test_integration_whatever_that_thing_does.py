from database import DBContext, query_all_elements
from airflow.models import DagBag
from src.lib.test_helpers import execute_dag, run_task_with_url
from .whatever_that_thing_does_dag import SCHEMA, TABLE


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("csv_download_to_postgres.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_with_no_errors(db_context: DBContext):
    credentials = db_context["credentials"]

    # If you rely on other dags putting data in the db before yours can run, run them here like so:

    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    # assert that your stuff does not crash

    assert (
        execute_dag(
            "example_csv_to_postgres",
            "2021-01-01",
            {"TARGET_DB": credentials["database"]},
        )
        == 0
    )
    res = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE}")

    # assert that the data in your table looks as expected given the test data that we have.

    res
    assert True
