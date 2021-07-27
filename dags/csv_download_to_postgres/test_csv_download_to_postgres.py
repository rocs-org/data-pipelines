import pandas as pd
import pytest
from .csv_download_to_postgres import (
    download_csv,
    download_csv_and_upload_to_postgres,
    write_dataframe_to_postgres,
)
from dags.database.db_context import (
    DB_Context,
    create_db_context,
    query_all_elements,
)

from dags.test_helpers import with_downloadable_csv

URL = "http://some.random.url/file.csv"
csv_content = """col1,col2,col3
1,hello,world
2,not,today
"""


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres(db_context):

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
def test_download_csv():

    result = download_csv(URL)

    assert result.equals(
        pd.DataFrame(
            columns=["col1", "col2", "col3"],
            data=[[1, "hello", "world"], [2, "not", "today"]],
        )
    )

    with pytest.raises(FileNotFoundError) as exception_info:
        download_csv("http://the.wrong.url/file.csv")

    assert "Cannot find file at" in str(exception_info.value)


@pytest.mark.usefixtures("db_context")
def test_write_df_to_postgres(db_context: DB_Context):

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
