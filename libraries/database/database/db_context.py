import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import ramda as R

from .types import (
    Cursor,
    Connection,
    DBCredentials,
    DBContext,
    DBContextWithCursor,
)


def open_cursor(context: DBContext) -> DBContextWithCursor:
    return R.apply_spec(
        {
            "credentials": R.prop("credentials"),
            "connection": R.prop("connection"),
            "cursor": R.pipe(
                R.prop("connection"), lambda conn: conn.cursor(cursor_factory=Cursor)
            ),
        }
    )(context)


def close_cursor(context: DBContextWithCursor) -> DBContext:
    return R.pipe(
        R.evolve({"cursor": R.tap(R.invoker(0, "close"))}),
        R.pick(["credentials", "connection"]),
    )(context)


def create_db_context(*_) -> DBContext:
    return R.pipe(
        lambda *_: _read_db_credentials_from_env(),
        _create_context_from_credentials,
    )("")


def teardown_db_context(context: DBContext) -> DBContext:
    context["connection"].commit()
    context["connection"].close()
    return context


def with_db_context(function, *args):
    context = create_db_context()
    result = function(context, *args)
    teardown_db_context(context)
    return result


def _read_db_credentials_from_env() -> DBCredentials:
    return {
        "user": os.environ["TARGET_DB_USER"],
        "password": os.environ["TARGET_DB_PW"],
        "database": os.environ["TARGET_DB"],
        "host": os.environ["TARGET_DB_HOSTNAME"],
        "port": int(os.environ["TARGET_DB_PORT"]),
    }


def _connect_to_db(credentials: DBCredentials) -> Connection:
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


@R.curry
def _set_isolation_level(isolation_level: int, connection: Connection) -> Connection:
    connection.set_isolation_level(isolation_level)
    return connection


def _create_db_context_with_autocommit() -> DBContext:
    return R.pipe(
        lambda *_: _read_db_credentials_from_env(),
        R.apply_spec(
            {
                "connection": R.pipe(
                    _connect_to_db, _set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                ),
                "credentials": R.identity,
            }
        ),
    )("")


def raiser(err):
    raise err
