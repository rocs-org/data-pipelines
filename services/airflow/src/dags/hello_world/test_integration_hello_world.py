import subprocess
from airflow.models import DagBag


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    process = subprocess.Popen(
        [
            "airflow",
            "dags",
            "backfill",
            "-s",
            execution_date,
            dag_id,
        ]
    )
    process.communicate()[0]
    return process.returncode


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("hello_world.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_with_no_errors():
    assert execute_dag("hello_world_etl", "2021-01-01") == 0
