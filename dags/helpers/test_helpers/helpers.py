import typing
import functools

import ramda as R
import responses
from io import StringIO
import subprocess
import os
import pytest
import json

from returns.curry import curry

from dags.database import (
    teardown_test_db_context,
    create_test_db_context,
    migrate,
)


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


def execute_dag(dag_id: str, execution_date: str, dag_config: dict = {}):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""

    subprocess.Popen(["airflow", "dags", "delete", dag_id, "-y"])

    process = subprocess.Popen(
        [
            "airflow",
            "dags",
            "backfill",
            "-v",
            "-s",
            execution_date,
            "-c",
            json.dumps(dag_config),
            dag_id,
        ],
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


@curry
def set_env_variable(name: str, value):
    os.environ[name] = value


def get_from_dag_conf(name: str):
    return R.pipe(R.path(["dag_run", "conf"]), lambda x: x.get(name))


@curry
def check_if_var_exists_in_dag_conf(name: str, kwargs):
    return R.try_catch(R.pipe(get_from_dag_conf(name), R.is_nil, R.not_func), R.F)(
        kwargs
    )


@curry
def set_env_variable_from_dag_config_if_present(name: str, kwargs):
    return R.if_else(
        check_if_var_exists_in_dag_conf(name),
        R.pipe(
            R.tap(
                R.pipe(
                    R.path(["dag_run", "conf"]),
                    lambda x: print("setting to env from dag_config: ", x),
                )
            ),
            get_from_dag_conf(name),
            set_env_variable(name),
        ),
        R.F,
    )(kwargs)


@curry
def insert_url_from_dag_conf(task_function, url, *args, **kwargs):
    print(R.path(["dag_run", "conf"], kwargs))
    return R.if_else(
        check_if_var_exists_in_dag_conf("URL"),
        R.pipe(
            get_from_dag_conf("URL"),
            lambda x: task_function(x, *args, **kwargs),
        ),
        lambda x: task_function(url, *args, **kwargs),
    )(kwargs)