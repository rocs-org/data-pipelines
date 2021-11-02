import pytest
import os
from . import create_test_db_context, migrate, teardown_test_db_context


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
