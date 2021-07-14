from typing import Any
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import random
import string
from returns.pipeline import pipe
from returns.curry import curry
import ramda as R
from typing import TypedDict

from psycopg2.extensions import connection, cursor


class Connection(connection):
    # this is a hack. Ramda wants to deepcopy objects,
    # so psycopg connections have to pretend to be able
    # to support this.

    def __deepcopy__(self, memo):
        return self


psycopg2.extensions.connection = Connection


class DB_Credentials(TypedDict):
    user: str
    password: str
    database: str
    host: str
    port: int


class DB_Context(TypedDict):
    credentials: DB_Credentials
    connection: Connection


def create_db_context() -> DB_Context:
    return pipe0(
        _read_db_credentials_from_env,
        _create_context_from_credentials,
    )()


def teardown_db_context(context: DB_Context) -> DB_Context:
    context["connection"].close()
    return context


def teardown_test_db_context(context: DB_Context) -> DB_Context:
    context["connection"].close()

    new_context = _create_db_context_with_autocommit()
    execute_sql(
        new_context, f"DROP DATABASE {R.path(['credentials', 'database'], context)};"
    )
    new_context["connection"].close()
    return context


def create_test_db_context() -> DB_Context:
    return pipe0(
        _create_db_context_with_autocommit,
        _create_random_db_name,
        _create_database,
        teardown_db_context,
        _update_connection_in_context,
    )()


@curry
def execute_sql(context, sql: str, data=None) -> None:
    return pipe(
        R.prop("connection"),
        _get_cursor_from_db_connection,
        _execute_sql_on_cursor(sql, data=data),
        lambda cursor: cursor.close(),
    )(context)


@curry
def query_one_element(context, sql: str) -> Any:
    cursor = _execute_sql_and_return_cursor(context, sql)
    res = cursor.fetchone()[0]
    cursor.close()
    return res


@curry
def query_all_elements(context, sql: str) -> Any:
    cursor = _execute_sql_and_return_cursor(
        context,
        sql,
    )
    res = cursor.fetchall()[0]
    cursor.close()
    return res


@curry
def pipe0(foo, *other_functions):
    # ugly hack because pipe does not work with functions without any args.
    def patched_pipe(*throwaway_args):
        return pipe(lambda x: foo(), *other_functions)("blub")

    return patched_pipe


def _read_db_credentials_from_env() -> DB_Credentials:
    return {
        "user": os.environ["TARGET_DB_USER"],
        "password": os.environ["TARGET_DB_PW"],
        "database": os.environ["TARGET_DB"],
        "host": os.environ["TARGET_DB_HOSTNAME"],
        "port": os.environ["TARGET_DB_PORT"],
    }


def _connect_to_db(credentials: DB_Credentials) -> Connection:
    return psycopg2.connect(connection_factory=Connection, **credentials)


_create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)

_update_connection_in_context = pipe(
    R.prop("credentials"), _create_context_from_credentials
)


def _create_random_db_name(context: DB_Context) -> DB_Context:
    return R.assoc_path(["credentials", "database"], _get_random_string(8).lower())(
        context
    )


@curry
def _execute_sql_and_return_cursor(context, sql):
    return pipe(
        R.prop("connection"),
        _get_cursor_from_db_connection,
        _execute_sql_on_cursor(sql),
    )(context)


def _get_cursor_from_db_connection(connection: Connection) -> cursor:
    return connection.cursor()


@curry
def _set_isolation_level(isolation_level: int, connection: Connection) -> Connection:
    connection.set_isolation_level(isolation_level)
    return connection


def _create_db_context_with_autocommit() -> DB_Context:
    return pipe0(
        _read_db_credentials_from_env,
        R.apply_spec(
            {
                "connection": pipe(
                    _connect_to_db, _set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                ),
                "credentials": R.identity,
            }
        ),
    )()


@curry
def _execute_sql_on_cursor(sql: str, c: cursor, data=None) -> cursor:
    c.execute(sql, data)
    return c


@curry
def _create_database(context: DB_Context) -> DB_Context:
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context


def _get_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(length))
