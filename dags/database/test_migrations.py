from dags.database.db_context import (
    create_test_db_context,
    execute_sql,
    query_all_elements,
    teardown_test_db_context,
)

import pytest
from psycopg2.errors import UndefinedTable
from dags.database.migrations import get_connection_string, migrate


def test_migrations():
    context = create_test_db_context()

    with pytest.raises(UndefinedTable):
        execute_sql(
            context,
            "INSERT INTO test_table (col1, col2, col3) VALUES(%s, %s, %s);",
            ("1", "err1", "err2"),
        )

    migrate(context)

    execute_sql(
        context,
        "INSERT INTO test_table (col1, col2, col3) VALUES(%s, %s, %s);",
        ("1", "val2", "val2"),
    )

    assert query_all_elements(context, "SELECT col1, col2, col3 FROM test_table") == [
        (1, "val2", "val2")
    ]

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
