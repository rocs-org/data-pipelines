import pandas as pd
from airflow.models import DagBag
from src.lib.test_helpers import execute_dag

from clickhouse_helpers import DBContext, query_dataframe

URL = "http://static-files/static/test_ch.csv"


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("csv_download_to_postgres.py")
    assert len(dag_bag.import_errors) == 0


def test_epoch_dag_executes_with_no_errors(
    ch_context: DBContext, thryve_clickhouse_context: DBContext
):

    # assert that the mock external table is populated (with two rows)
    external_data = query_dataframe(
        thryve_clickhouse_context,
        "SELECT * FROM raw_DynamicEpochValue",
    )
    assert len(external_data) == 2

    # assert that the dag executes without errors
    assert (
        execute_dag(
            "load_epoch_to_clickhouse",
            "2021-09-02",
            {
                "external_table": "raw_DynamicEpochValue",
                "prefix": "",
                "CLICKHOUSE_DB": ch_context["credentials"]["database"],
                "EXTERNAL_CLICKHOUSE_DB": thryve_clickhouse_context["credentials"][
                    "database"
                ],
            },
        )
        == 0
    )

    # assert that the data is loaded into the local table (only one row with the correct date)
    local_data = query_dataframe(
        ch_context,
        "SELECT * FROM raw_DynamicEpochValue",
    )
    assert len(local_data) == 1
    assert local_data["createdAt"].values[0] == pd.Timestamp("2021-09-01 10:14:47")
