from airflow.models import DagBag
from dags.database import DBContext, query_all_elements
from dags.helpers.test_helpers import execute_dag
from psycopg2 import sql
from dags.nuts_regions.dag import SCHEMA, TABLE

URL = "http://static-files/static/NUTS2021.xlsx"


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_and_writes_entries_to_DB(db_context: DBContext):
    credentials = db_context["credentials"]

    assert (
        execute_dag(
            "nuts_regions",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": URL},
        )
        == 0
    )

    res = query_all_elements(
        db_context,
        sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(SCHEMA), sql.Identifier(TABLE)
        ),
    )

    assert len(res) == 2121
