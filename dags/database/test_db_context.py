from dags.database.migrations import migrate
from os import environ
import pytest
from psycopg2.errors import OperationalError
import ramda as R
from returns.pipeline import pipe
from .db_context import (
    _connect_to_db,
    close_cursor,
    create_db_context,
    create_test_db_context,
    pipe0,
    _read_db_credentials_from_env,
    query_all_elements,
    query_one_element,
    teardown_db_context,
    execute_sql,
    teardown_test_db_context,
    open_cursor,
)


def test_create_db_context():

    db_context = create_db_context()

    assert type(db_context["credentials"]) == dict

    # assert that db_context contains a connection
    # assert type(db_context["connection"]) == psycopg2.extensions.connection

    # assert that the connection is open
    assert db_context["connection"].closed == 0


def test_teardown_db_context():

    db_context = pipe0(create_db_context, teardown_db_context)()
    # assert that connection is closed after teardown
    assert db_context["connection"].closed == 1


def test_db_connection_read_write():

    db_context = create_test_db_context()
    migrate(db_context)

    # db in credentials is NOT "production" database
    assert R.path(["credentials", "database"], db_context) != environ["TARGET_DB"]

    # db connection in db_context is actually to the one specified by 'database' in 'credentials'
    assert query_one_element(db_context, "SELECT current_database()") == R.path(
        ["credentials", "database"], db_context
    )

    # assert migrations have run and read write is working
    execute_sql(
        db_context,
        "INSERT INTO test_table (col1, col2, col3) VALUES(%s, %s, %s);",
        (1, "Hello", "World!"),
    )
    assert (
        query_all_elements(
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
    with pytest.raises(OperationalError):
        pipe(R.prop("credentials"), _connect_to_db)(context)


def test_read_credentials():
    credentials = _read_db_credentials_from_env()
    assert credentials["user"] == environ["TARGET_DB_USER"]


def test_open_cursor(db_context):

    with_cursor = open_cursor(db_context)
    assert with_cursor["cursor"] is not None
    assert with_cursor["cursor"].closed == 0


def test_close_cursor(db_context):

    with_cursor = open_cursor(db_context)

    cursor = with_cursor["cursor"]

    assert cursor.closed == 0

    close_cursor(with_cursor)

    assert cursor.closed == 1


# is Ramda working as expected?
def test_invoker():
    class TestClass:
        def one(self):
            return 1

    one = R.invoker(0, "one")(TestClass())
    assert one == 1


def test_use_with_lambda():
    def add(x, y):
        return x + y

    two = R.use_with(R.add, [lambda x: x, R.identity])(1, 1)
    assert two == 2

    three = R.use_with(R.add, [R.pipe(R.identity), R.identity])(1, 2)
    assert three == 3
