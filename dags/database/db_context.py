from typing import Any, List
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import random
import string
from returns.pipeline import pipe
from returns.curry import curry
import ramda as R
from typing import TypedDict

from psycopg2.extensions import connection, cursor


# this is a hack. Ramda wants to deepcopy objects,
# so psycopg connections have to pretend to be able
# to support this.
class Connection(connection):
    def __deepcopy__(self, memo):
        return self


class Cursor(cursor):
    def __deepcopy__(self, memo):
        return self


psycopg2.extensions.connection = Connection
psycopg2.extensions.cursor = Cursor


class DB_Credentials(TypedDict):
    user: str
    password: str
    database: str
    host: str
    port: int


class DB_Context(TypedDict):
    credentials: DB_Credentials
    connection: Connection


class DB_Context_with_cursor(TypedDict):
    credentials: DB_Credentials
    connection: Connection
    cursor: Cursor


def open_cursor(context: DB_Context) -> DB_Context_with_cursor:
    return R.apply_spec(
        {
            "credentials": R.prop("credentials"),
            "connection": R.prop("connection"),
            "cursor": pipe(
                R.prop("connection"), lambda conn: conn.cursor(cursor_factory=Cursor)
            ),
        }
    )(context)


def close_cursor(context: DB_Context_with_cursor) -> DB_Context:
    return R.pipe(
        R.evolve({"cursor": R.tap(R.invoker(0, "close"))}),
        R.pick(["credentials", "connection"]),
    )(context)


def rollback_changes(context: DB_Context_with_cursor) -> DB_Context_with_cursor:
    return R.tap(R.pipe(R.prop("connection"), R.invoker(0, "rollback")))(context)


def commit_changes(context: DB_Context_with_cursor) -> DB_Context_with_cursor:
    return R.tap(R.pipe(R.prop("connection"), R.invoker(0, "commit")))(context)


def cleanup_db_connection(context: DB_Context_with_cursor) -> int:
    return R.pipe(rollback_changes, close_cursor)(context)


@curry
def execute_values(context: DB_Context, query: str, tuples: List[Any]) -> DB_Context:
    return R.unapply(
        R.pipe(
            R.tap(
                R.apply(
                    R.use_with(
                        psycopg2.extras.execute_values,
                        [R.pipe(open_cursor, R.prop("cursor")), R.identity, R.identity],
                    )
                )
            ),
            R.head,
            commit_changes,
            close_cursor,
        )
    )(context, query, tuples)


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
    return R.pipe(
        R.try_catch(
            R.apply(
                R.use_with(
                    _execute_sql_on_cursor, [open_cursor, R.identity, R.identity]
                )
            ),
            R.unapply(
                R.pipe(
                    R.tap(
                        R.pipe(
                            R.nth(1),
                            R.head,
                            rollback_changes,
                        )
                    ),
                    R.head,
                    raiser,
                )
            ),
        ),
        close_cursor,
    )([context, sql, data])


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
    res = cursor.fetchall()
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
        "port": int(os.environ["TARGET_DB_PORT"]),
    }


def _connect_to_db(credentials: DB_Credentials) -> Connection:
    return psycopg2.connect(connection_factory=Connection, **credentials)


_create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)

_update_connection_in_context = R.pipe(
    R.prop("credentials"), _create_context_from_credentials
)


def _create_random_db_name(context: DB_Context) -> DB_Context:
    return R.assoc_path(["credentials", "database"], _get_random_string(8).lower())(
        context
    )


@curry
def _execute_sql_and_return_cursor(context: DB_Context, sql: str) -> Cursor:
    return R.pipe(
        R.apply(R.use_with(_execute_sql_on_cursor, [open_cursor, R.identity])),
        R.prop("cursor"),
    )([context, sql])


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
def _execute_sql_on_cursor(
    context: DB_Context_with_cursor, sql: str, data=None
) -> DB_Context_with_cursor:
    context["cursor"].execute(sql, data)
    return context


@curry
def _create_database(context: DB_Context) -> DB_Context:
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context


def _get_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(length))


def raiser(err):
    raise err
