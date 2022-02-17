from airflow.models import DagBag

from clickhouse_helpers import DBContext, query_dataframe
from src.lib.test_helpers import execute_dag


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_epoch_dag_writes_correct_results_to_db(ch_context: DBContext):
    credentials = ch_context["credentials"]

    assert (
        execute_dag(
            "datenspende_epoch_data_import_v1",
            "2021-01-01",
            {"CLICKHOUSE_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )
    answers_from_db = query_dataframe(ch_context, "SELECT * FROM vital_data_epoch")
    assert len(answers_from_db) == 300


THRYVE_FTP_URL = "http://static-files/thryve/exportEpoch.7z"
