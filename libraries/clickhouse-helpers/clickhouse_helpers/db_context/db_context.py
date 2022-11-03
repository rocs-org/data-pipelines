import os

import ramda as R
from clickhouse_driver import Client

from clickhouse_helpers.types import (
    DBCredentials,
    DBContext,
)


def create_db_context(**kwargs) -> DBContext:
    return R.pipe(
        lambda kwargs: _read_db_credentials_from_env(**kwargs),
        create_context_from_credentials,
    )(kwargs)


def teardown_db_context(context: DBContext) -> DBContext:
    context["connection"].disconnect()
    return context


def with_db_context(function, *args):
    context = create_db_context()
    result = function(context, *args)
    teardown_db_context(context)
    return result


def _read_db_credentials_from_env(**kwargs) -> DBCredentials:
    prefix = kwargs.pop("env_prefix", "CLICKHOUSE")
    return {
        "user": os.environ[f"{prefix}_USER"],
        "password": os.environ[f"{prefix}_PASSWORD"],
        "database": os.environ[f"{prefix}_DB"],
        "host": os.environ[f"{prefix}_HOSTNAME"],
        "port": int(os.environ[f"{prefix}_PORT"]),
        **kwargs,  # allow overriding of the above and adding additional settings for the connection
    }


def _connect_to_db(credentials: DBCredentials) -> Client:
    return Client(**credentials, settings={"use_numpy": True})


def raiser(err):
    raise err


create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)
