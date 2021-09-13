from dags.database import DBContext
from dags.database.execute_sql import query_all_elements
from airflow.models import DagBag

from dags.helpers.test_helpers import execute_dag
from dags.corona_cases.cases import CASES_ARGS

URL = "http://static-files/static/coronacases.csv"
[_ , SCHEMA, TABLE] = CASES_ARGS


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_with_no_errors(db_context: DBContext):
    credentials = db_context["credentials"]

    assert (
        execute_dag(
            "corona_cases",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": URL},
        )
        == 0
    )
    res = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE}")

    assert len(res) == 9
