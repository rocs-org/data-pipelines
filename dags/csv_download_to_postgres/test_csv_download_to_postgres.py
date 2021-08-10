import pandas as pd
import pytest
import os
import psycopg2
from .csv_download_to_postgres import (
    download_csv_and_upload_to_postgres,
    write_dataframe_to_postgres,
)
from dags.database.db_context import (
    create_db_context,
)
from ..database import DBContext
from ..database.execute_sql import query_all_elements

from dags.helpers.test_helpers import with_downloadable_csv

URL = "http://some.random.url/file.csv"
csv_content = """col1,col2,col3
1,hello,world
2,not,today
"""


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres_happy_path(db_context):
    table = "test_table"

    download_csv_and_upload_to_postgres(URL, table)

    context = create_db_context()
    results = query_all_elements(context, f"SELECT col1, col2, col3 FROM {table}")

    assert len(results) == 2
    assert results == [
        (1, "hello", "world"),
        (2, "not", "today"),
    ]


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres_picks_up_injected_db_name(db_context):
    table = "test_table"

    with pytest.raises(psycopg2.OperationalError) as exception_info:
        download_csv_and_upload_to_postgres(
            URL, table, dag_run={"conf": {"TARGET_DB": "rando_name"}}
        )

    assert 'database "rando_name" does not exist' in str(exception_info.value)
    assert os.environ["TARGET_DB"] == "rando_name"


@pytest.mark.usefixtures("db_context")
def test_write_df_to_postgres(db_context: DBContext):
    df = pd.DataFrame(
        columns=["col1", "col2", "col3"],
        data=[[1, "hello", "world"], [2, "not", "today"]],
    )

    table = "test_table"

    res = write_dataframe_to_postgres(db_context, table)(df)
    print(res)

    assert query_all_elements(db_context, f"SELECT col1, col2, col3 FROM {table}") == [
        (1, "hello", "world"),
        (2, "not", "today"),
    ]
