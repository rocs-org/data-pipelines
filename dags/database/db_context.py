import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import random
import string
from returns.pipeline import pipe
from returns.curry import curry
import ramda as R

from psycopg2.extensions import connection, cursor


def teardown_db_context(context):
    context["connection"].close()
    return context


@curry
def execute_sql(context, sql, data=None):
    return pipe(
        R.prop("connection"),
        _get_cursor_from_db_connection,
        _execute_sql_on_cursor(sql, data=data),
        lambda c: c.close(),
    )(context)


@curry
def execute_sql_and_return_cursor(context, sql):
    return pipe(
        R.prop("connection"),
        _get_cursor_from_db_connection,
        _execute_sql_on_cursor(sql),
    )(context)


@curry
def query_one_element(context, sql):
    cursor = execute_sql_and_return_cursor(context, sql)
    res = cursor.fetchone()[0]
    cursor.close()
    return res


@curry
def query_all_elements(context, sql):
    cursor = execute_sql_and_return_cursor(
        context,
        sql,
    )
    res = cursor.fetchall()[0]
    cursor.close()
    return res


def create_test_db_context():
    return pipe0(
        _create_db_context_with_autocommit,
        R.assoc_path(["credentials", "database"], _get_random_string(8).lower()),
        _create_database,
        teardown_db_context,
        recreate_context,
    )()


class Connection(connection):
    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


psycopg2.extensions.connection = Connection


@curry
def pipe0(foo, *other_functions):
    # ugly hack because pipe does not work with functions without any args.
    def patched_pipe(*throwaway_args):
        return pipe(lambda x: foo(), *other_functions)("blub")

    return patched_pipe


def _read_db_credentials_from_env():
    return {
        "user": os.environ["TARGET_DB_USER"],
        "password": os.environ["TARGET_DB_PW"],
        "database": os.environ["TARGET_DB"],
        "host": os.environ["TARGET_DB_HOSTNAME"],
        "port": os.environ["TARGET_DB_PORT"],
    }


def _connect_to_db(credentials):
    return psycopg2.connect(connection_factory=Connection, **credentials)


def _get_cursor_from_db_connection(connection):
    return connection.cursor()


@curry
def _set_isolation_level(isolation_level, connection):
    connection.set_isolation_level(isolation_level)
    return connection


def _create_db_context_with_autocommit():
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
def _execute_sql_on_cursor(sql, c: cursor, data=None) -> cursor:
    c.execute(sql, data)
    return c


@curry
def _create_database(context):
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context


def _get_random_string(length):
    return "".join(random.choice(string.ascii_letters) for i in range(length))


create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)

recreate_context = pipe(R.prop("credentials"), create_context_from_credentials)

create_db_context = pipe0(
    _read_db_credentials_from_env,
    create_context_from_credentials,
)
