from os import environ
import pytest
import psycopg2
from psycopg2.errors import OperationalError
import ramda as R
from returns.pipeline import pipe
from .db_context import (
    _connect_to_db,
    create_db_context,
    create_test_db_context,
    pipe0,
    _read_db_credentials_from_env,
    query_all_elements,
    query_one_element,
    teardown_db_context,
    execute_sql,
    teardown_test_db_context,
)


def test_create_db_context():

    db_context = create_db_context()

    assert type(db_context["credentials"]) == dict

    # assert that db_context contains a connection
    assert type(db_context["connection"]) == psycopg2.extensions.connection

    # assert that the connection is open
    assert db_context["connection"].closed == 0


def test_teardown_db_context():

    db_context = pipe0(create_db_context, teardown_db_context)()
    # assert that connection is closed after teardown
    assert db_context["connection"].closed == 1


@pytest.fixture
def db_context():
    context = create_test_db_context()
    yield context
    teardown_test_db_context(context)


def test_db_connection_read_write(db_context):

    # db in credentials is NOT "production" database
    assert R.path(["credentials", "database"], db_context) != environ["TARGET_DB"]

    # db connection in db_context is actually to the one specified by 'database' in 'credentials'
    assert query_one_element(db_context, "SELECT current_database()") == R.path(
        ["credentials", "database"], db_context
    )

    # assert read write is working
    execute_sql(
        db_context,
        """CREATE TABLE test_table (
            id serial PRIMARY KEY,
            c1 varchar(255) NOT NULL,
            c2 varchar(255) NOT NULL
            );
        """,
    )
    execute_sql(
        db_context,
        "INSERT INTO test_table (c1, c2) VALUES(%s, %s);",
        ("Hello", "World!"),
    )
    assert (
        query_all_elements(
            db_context,
            """
             SELECT * FROM test_table;
        """,
        )
        == (1, "Hello", "World!")
    )


def test_teardown_test_db_context():
    context = create_test_db_context()
    teardown_test_db_context(context)
    with pytest.raises(OperationalError):
        pipe(R.prop("credentials"), _connect_to_db)(context)


def test_read_credentials():
    credentials = _read_db_credentials_from_env()
    assert credentials["user"] == environ["TARGET_DB_USER"]
