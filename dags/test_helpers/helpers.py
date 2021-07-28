import typing
import functools
import responses
from io import StringIO
import subprocess
import os
import pytest
from dags.database import (
    teardown_test_db_context,
    create_test_db_context,
    migrate,
)
import ramda as R


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


def execute_dag(dag_id: str, execution_date: str, environment: dict = {}):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    env = R.merge(os.environ.copy(), environment)
    print(env)
    process = subprocess.Popen(
        [
            "airflow",
            "dags",
            "backfill",
            "-s",
            execution_date,
            dag_id,
        ],
        env=env,
    )
    process.communicate()[0]
    return process.returncode


def with_downloadable_csv(
    foo: typing.Callable = None,
    url: str = None,
    content: str = None,
):
    if foo is None:
        return functools.partial(with_downloadable_csv, url=url, content=content)

    @functools.wraps(foo)
    @responses.activate
    def wrapped_foo(*args: typing.Any, **kwargs: typing.Any):
        responses.add(
            responses.GET,
            url,
            status=200,
            content_type="file",
            body=StringIO(content).read(),
            headers={"Content-disposition": "attachment; filename=file.csv"},
            stream=True,
        )
        return foo(*args, **kwargs)  # type: ignore

    return wrapped_foo
