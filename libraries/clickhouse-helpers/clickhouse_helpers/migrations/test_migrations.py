import pandas as pd
import pytest
from clickhouse_driver.errors import ServerException

from clickhouse_helpers import (
    query_dataframe,
    insert_dataframe,
    teardown_test_db_context,
    create_test_db_context,
)
from clickhouse_helpers.migrations.migrations import (
    get_connection_string,
    migrate,
)


def test_migrate_runs_migrations_on_db():
    context = create_test_db_context()

    with pytest.raises(ServerException):
        context["connection"].execute(
            f"INSERT INTO {context['credentials']['database']}.test_table (col1, col2, col3) VALUES",
            {"col1": 1, "col2": "err1", "col3": "err2"},
        )

    migrate(context)

    insert_dataframe(
        context,
        "test_table",
        TEST_DF,
    )

    res = query_dataframe(
        context,
        f"SELECT * FROM {context['credentials']['database']}.test_table;",
    )

    assert (res.values == TEST_DF.values).all()

    teardown_test_db_context(context)


def test_get_connection_string():
    connection_string = get_connection_string(
        {
            "credentials": {
                "user": "test_user",
                "password": "pw",
                "database": "db",
                "host": "test_host",
                "port": 5432,
            }
        }
    )
    assert connection_string == "postgres://test_user:pw@test_host:5432/db"


TEST_DF = pd.DataFrame(
    {"id": [1, 2], "col1": [3, 4], "col2": ["a", "b"], "col3": ["c", "d"]}
)
