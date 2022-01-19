from database import DBContext, query_all_elements
from airflow.models import DagBag
from datetime import date

from src.lib.test_helpers import execute_dag


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_dag_writes_correct_results_to_db(db_context: DBContext):
    credentials = db_context["credentials"]

    assert (
        execute_dag(
            "datenspende_vitaldata_v2",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )
    answers_from_db = query_all_elements(
        db_context, "SELECT * FROM datenspende.vitaldata;"
    )
    assert answers_from_db[-1] == (
        200,
        date(2021, 10, 21),
        65,
        70,
        6,
        1635228999300,
        120,
    )
    assert len(answers_from_db) == 20

    answers_from_db = query_all_elements(
        db_context, "SELECT * FROM datenspende.steps_ct;"
    )
    assert len(answers_from_db) == 2

THRYVE_FTP_URL = "http://static-files/thryve/export.7z"
