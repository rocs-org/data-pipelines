import subprocess
import json
from datetime import date

from airflow.models import DagBag

from postgres_helpers import DBContext, query_all_elements
from src.lib.test_helpers import execute_dag


def trigger_dag(dag_id: str, dag_config: dict = {}):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""

    # subprocess.Popen(["airflow", "dags", "delete", dag_id, "-y"])

    process = subprocess.Popen(
        [
            "airflow",
            "dags",
            "trigger",
            "-c",
            json.dumps(dag_config),
            dag_id,
        ],
    )
    process.communicate()

    return process.returncode


def test_datenspende_onetime_vitals_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_onetime_vitals_dag_writes_correct_results_to_db(
    pg_context: DBContext,
):
    credentials = pg_context["credentials"]

    assert (
        trigger_dag(
            "datenspende_vitaldata_onetime_update",
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


THRYVE_FTP_URL = "http://static-files/thryve/export_scripps.7z"
