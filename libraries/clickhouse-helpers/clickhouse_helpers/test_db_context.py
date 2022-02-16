from os import environ
import pytest
import ramda as R
from clickhouse_driver.errors import ServerException
from .db_context import (
    create_db_context,
    _read_db_credentials_from_env,
    teardown_db_context,
)
from . import (
    execute_sql,
    teardown_test_db_context,
    create_test_db_context,
    migrate,
    DBContext,
)


def test_db_context_fixture_supplies_context_with_migrations_applied(
    db_context: DBContext,
):
    # assert migrations have run and read write is working
    execute_sql(
        db_context,
        "INSERT INTO test_table (col1, col2, col3) VALUES",
        [(1, "Hello", "World!")],
    )
    assert (
        execute_sql(
            db_context,
            """
             SELECT col1, col2, col3 FROM test_table;
        """,
        )
        == [(1, "Hello", "World!")]
    )


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
    execute_sql(
        db_context,
        "INSERT INTO test_table (col1, col2, col3) VALUES",
        [(1, "Hello", "World!")],
    )
    assert (
        execute_sql(
            db_context,
            """
             SELECT col1, col2, col3 FROM test_table;
        """,
        )
        == [(1, "Hello", "World!")]
    )

    teardown_test_db_context(db_context)


def test_teardown_test_db_context():
    context = create_test_db_context()
    teardown_test_db_context(context)
    with pytest.raises(ServerException) as e:
        execute_sql(context, "SELECT * FROM test_table")
    assert f"Database {context['credentials']['database']} doesn't exist" in str(
        e.value
    )


def test_read_credentials():
    credentials = _read_db_credentials_from_env()
    assert credentials["user"] == environ["CLICKHOUSE_USER"]
