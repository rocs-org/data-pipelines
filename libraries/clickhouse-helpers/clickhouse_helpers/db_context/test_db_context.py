from os import environ
import pandas as pd
import pytest
import ramda as R
from clickhouse_driver.errors import ServerException

from clickhouse_helpers import (
    execute_sql,
    query_dataframe,
    insert_dataframe,
    teardown_test_db_context,
    create_test_db_context,
    migrate,
    DBContext,
)
from .db_context import (
    create_db_context,
    _read_db_credentials_from_env,
    teardown_db_context,
)


def test_db_context_fixture_supplies_context_with_migrations_applied(
    db_context: DBContext,
):
    # assert migrations have run and read write is working
    insert_dataframe(
        db_context,
        "test_table",
        TEST_DF,
    )
    assert (
        query_dataframe(
            db_context,
            """
             SELECT * FROM test_table;
            """,
        ).values
        == TEST_DF.values
    ).all()


def test_create_db_context():

    db_context = create_db_context()
    print(db_context["credentials"])
    assert type(db_context["credentials"]) == dict

    assert db_context["connection"].connection.connected is False

    db_context["connection"].connection.connect()

    assert db_context["connection"].connection.connected is True

    db_context["connection"].connection.disconnect()

    assert db_context["connection"].connection.connected is False


def test_teardown_db_context():

    db_context = R.pipe(lambda *_: create_db_context(), teardown_db_context)("")
    # assert that connection is closed after teardown
    assert db_context["connection"].connection.connected is False


def test_db_connection_read_write():

    db_context = create_test_db_context()
    migrate(db_context)

    # db in credentials is NOT "production" database
    assert R.path(["credentials", "database"], db_context) != environ["CLICKHOUSE_DB"]

    # db connection in db_context is actually to the one specified by 'database' in 'credentials'
    assert execute_sql(db_context, "SELECT currentDatabase()")[0][0] == R.path(
        ["credentials", "database"], db_context
    )

    # assert migrations have run and read write is working
    insert_dataframe(db_context, "test_table", TEST_DF)

    res = query_dataframe(
        db_context,
        """
             SELECT * FROM test_table;
            """,
    )

    assert (res.values == TEST_DF.values).all()

    teardown_test_db_context(db_context)


def test_teardown_test_db_context():
    context = create_test_db_context()
    teardown_test_db_context(context)
    with pytest.raises(ServerException) as e:
        query_dataframe(context, "SELECT * FROM test_table")
    assert f"Database {context['credentials']['database']} doesn't exist" in str(
        e.value
    )


def test_read_credentials():
    credentials = _read_db_credentials_from_env()
    assert credentials["user"] == environ["CLICKHOUSE_USER"]


TEST_DF = pd.DataFrame(
    {"id": [0, 1], "col1": [1, 2], "col2": ["Hello", "b"], "col3": ["World!", "c"]}
)
