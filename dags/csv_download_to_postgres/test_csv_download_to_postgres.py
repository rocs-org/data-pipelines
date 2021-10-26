import pytest
import os
import psycopg2
from .csv_download_to_postgres import (
    download_csv_and_upload_to_postgres,
)
from dags.database.db_context import (
    create_db_context,
)
from ..database.execute_sql import query_all_elements

URL = "http://static-files/static/test.csv"


def test_download_csv_and_write_to_postgres_happy_path(db_context):
    table = "test_table"

    download_csv_and_upload_to_postgres(URL, table)

    context = create_db_context()
    results = query_all_elements(
        context, f"SELECT col1, col2, col3 FROM test_tables.{table}"
    )

    assert len(results) == 2
    assert results == [
        (1, "hello", "world"),
        (2, "not", "today"),
    ]


def test_download_csv_and_write_to_postgres_picks_up_injected_db_name(db_context):
    table = "test_table"

    with pytest.raises(psycopg2.OperationalError) as exception_info:
        download_csv_and_upload_to_postgres(
            URL, table, dag_run={"conf": {"TARGET_DB": "rando_name"}}
        )

    assert 'database "rando_name" does not exist' in str(exception_info.value)
    assert os.environ["TARGET_DB"] == "rando_name"
