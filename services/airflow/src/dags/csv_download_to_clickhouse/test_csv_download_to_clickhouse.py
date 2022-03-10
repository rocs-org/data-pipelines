import os

import pandas as pd
import pytest
from pandahouse.http import ClickhouseException

from clickhouse_helpers import query_dataframe
from .csv_download_to_clickhouse import (
    download_csv_and_upload_to_clickhouse,
)

URL = "http://static-files/static/test.csv"


def test_download_csv_and_write_to_clickhouse_happy_path(ch_context):
    table = "test_table"

    download_csv_and_upload_to_clickhouse(URL, table)

    results = query_dataframe(ch_context, f"SELECT col1, col2, col3 FROM {table}")

    assert len(results) == 2
    assert (
        results.values
        == pd.DataFrame(
            {"col1": [1, 2], "col2": ["hello", "not"], "col3": ["world", "today"]}
        ).values
    ).all()


def test_download_csv_and_write_to_clickhouse_picks_up_injected_db_name(ch_context):
    table = "test_table"

    with pytest.raises(ClickhouseException) as exception_info:
        download_csv_and_upload_to_clickhouse(
            URL, table, dag_run={"conf": {"CLICKHOUSE_DB": "rando_name"}}
        )

    assert "Database rando_name doesn't exist" in str(exception_info.value)
    assert os.environ["CLICKHOUSE_DB"] == "rando_name"
