import random
import string
import pytest
import os
import ramda as R

from .types import DBContext
from .execute_sql import execute_sql, _create_database
from .db_context import (
    teardown_db_context,
    _create_db_context_with_autocommit,
    _update_connection_in_context,
)
from .migrations import migrate


@pytest.fixture
def db_context():
    context = create_test_db_context()
    migrate(context)

    credentials = context["credentials"]
    main_db = os.environ["TARGET_DB"]
    os.environ["TARGET_DB"] = credentials["database"]

    yield context

    os.environ["TARGET_DB"] = main_db
    teardown_test_db_context(context)


def teardown_test_db_context(context: DBContext) -> DBContext:
    context["connection"].close()

    new_context = _create_db_context_with_autocommit()
    execute_sql(
        new_context, f"DROP DATABASE {R.path(['credentials', 'database'], context)};"
    )
    new_context["connection"].close()
    return context


def create_test_db_context() -> DBContext:
    return R.pipe(
        lambda *_: _create_db_context_with_autocommit(),
        _create_random_db_name,
        _create_database,
        teardown_db_context,
        _update_connection_in_context,
    )("")


def _create_random_db_name(context: DBContext) -> DBContext:
    return R.assoc_path(["credentials", "database"], _get_random_string(8).lower())(
        context
    )


def _get_random_string(length: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(length))
