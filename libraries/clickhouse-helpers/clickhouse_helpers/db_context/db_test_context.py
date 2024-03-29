import os
import random
import string

import pytest
import ramda as R

from clickhouse_helpers.db_context import (
    create_db_context,
)
from clickhouse_helpers.db_context.db_context import _connect_to_db
from clickhouse_helpers.execute_sql.execute_sql import execute_sql, _create_database
from clickhouse_helpers.migrations import migrate
from clickhouse_helpers.types import DBContext


@pytest.fixture
def db_context(**kwargs):
    context = create_test_db_context(**kwargs)
    migrate(context)

    test_credentials = context["credentials"]
    main_db = os.environ["CLICKHOUSE_DB"]
    os.environ["CLICKHOUSE_DB"] = test_credentials["database"]

    yield context

    os.environ["CLICKHOUSE_DB"] = main_db
    teardown_test_db_context(context)


def teardown_test_db_context(context: DBContext) -> DBContext:
    execute_sql(
        context, f"DROP DATABASE {R.path(['credentials', 'database'], context)};"
    )
    context["connection"].disconnect()
    return context


def create_test_db_context(**kwargs) -> DBContext:
    return R.pipe(
        lambda kwargs: create_db_context(**kwargs),
        _create_random_db_name,
        _create_database,
        _update_connection_in_context,
    )(kwargs)


def _create_random_db_name(context: DBContext) -> DBContext:
    return R.assoc_path(["credentials", "database"], _get_random_string(8).lower())(
        context
    )


def _get_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(length))


_create_context_from_credentials = R.apply_spec(
    {
        "connection": _connect_to_db,
        "credentials": R.identity,
    }
)
_update_connection_in_context = R.pipe(
    R.prop("credentials"), _create_context_from_credentials
)
