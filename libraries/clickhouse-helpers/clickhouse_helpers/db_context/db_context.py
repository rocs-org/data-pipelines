import os

import ramda as R
from clickhouse_driver import Client

from clickhouse_helpers.types import (
    DBCredentials,
    DBContext,
)


def create_db_context(*_) -> DBContext:
    return R.pipe(
        lambda *_: _read_db_credentials_from_env(),
        create_context_from_credentials,
    )("")


def teardown_db_context(context: DBContext) -> DBContext:
    context["connection"].disconnect()
    return context


def with_db_context(function, *args):
    context = create_db_context()
    result = function(context, *args)
    teardown_db_context(context)
    return result


def _read_db_credentials_from_env() -> DBCredentials:
    return {
        "user": os.environ["CLICKHOUSE_USER"],
        "password": os.environ["CLICKHOUSE_PASSWORD"],
        "database": os.environ["CLICKHOUSE_DB"],
        "host": os.environ["CLICKHOUSE_HOSTNAME"],
        "port": int(os.environ["CLICKHOUSE_PORT"]),
    }


def _connect_to_db(credentials: DBCredentials) -> Client:
    return Client(**credentials)


def raiser(err):
    raise err


create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)
