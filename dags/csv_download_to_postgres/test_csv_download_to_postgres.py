import pandas as pd
import pytest
import os
import psycopg2
from .csv_download_to_postgres import (
    download_csv,
    download_csv_and_upload_to_postgres,
    write_dataframe_to_postgres,
)
from ..test_helpers.helpers import (
    set_env_variable,
    check_if_var_exists_in_dag_conf,
    set_env_variable_from_dag_config_if_present,
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


def test_check_if_var_exists_in_dag_conf():
    assert (
        check_if_var_exists_in_dag_conf("var", {"dag_run": {"conf": {"var": "value"}}})
        is True
    )
    assert (
        check_if_var_exists_in_dag_conf(
            "varrr", {"dag_run": {"conf": {"var": "value"}}}
        )
        is False
    )
    assert check_if_var_exists_in_dag_conf("var", {"dag_run": {}}) is False
    assert check_if_var_exists_in_dag_conf("var", {}) is False


def test_set_env_variable():
    set_env_variable("abc", "value")
    assert os.environ.get("abc") == "value"


def test_set_env_variable_from_dag_config():
    set_env_variable_from_dag_config_if_present("var")(
        {"dag_run": {"conf": {"var": "value"}}}
    )
    assert os.environ.get("var") == "value"


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
