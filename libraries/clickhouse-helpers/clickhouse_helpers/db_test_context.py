import random
import string
import pytest
import os
import ramda as R

from .types import DBContext
from .execute_sql import execute_sql, _create_database
from .db_context import create_db_context, _update_connection_in_context
from .migrations import migrate


@pytest.fixture
def db_context():
    context = create_test_db_context()
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


def create_test_db_context() -> DBContext:
    return R.pipe(
        lambda *_: create_db_context(),
        _create_random_db_name,
        _create_database,
        _update_connection_in_context,
    )("")


def _create_random_db_name(context: DBContext) -> DBContext:
    return R.assoc_path(["credentials", "database"], _get_random_string(8).lower())(
        context
    )


def _get_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(length))
