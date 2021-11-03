from datetime import datetime
import ramda as R
import subprocess
import os
import json

from airflow.models import DagBag, TaskInstance
from airflow.utils.types import DagRunType
from returns.curry import curry


def execute_dag(dag_id: str, execution_date: str, dag_config: dict = {}):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""

    subprocess.Popen(["airflow", "dags", "delete", dag_id, "-y"])

    process = subprocess.Popen(
        [
            "airflow",
            "dags",
            "backfill",
            "-x",  # SAVE YOURSELF THE HEADACHE, DO NOT PICKLE!!!
            "-v",
            "-s",
            execution_date,
            "-c",
            json.dumps(dag_config),
            dag_id,
        ],
    )
    process.communicate()

    return process.returncode


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
def if_var_exists_in_dag_conf_use_as_first_arg(
    var_name, task_function, url, *args, **kwargs
):
    print(R.path(["dag_run", "conf"], kwargs))
    return R.if_else(
        check_if_var_exists_in_dag_conf(var_name),
        R.pipe(
            get_from_dag_conf(var_name),
            lambda x: task_function(x, *args, **kwargs),
        ),
        lambda x: task_function(url, *args, **kwargs),
    )(kwargs)


def create_task_instance(dag_id: str, task_id: str, url: str = None):
    dag = DagBag().get_dag(dag_id)
    execution_date = datetime.now()
    dr = dag.create_dagrun(
        state="running", run_type=DagRunType("manual"), execution_date=execution_date
    )

    task = dag.get_task(task_id)
    if url:
        task.op_args[0] = url

    return task, TaskInstance(task=task, run_id=dr.run_id)


def run_task_with_url(dag_id: str, task_id: str, url: str):

    task, task_instance = create_task_instance(dag_id, task_id, url)

    task_instance.get_template_context()
    res = task.prepare_for_execution().execute(task_instance.get_template_context())

    return res


def get_task_context(dag_id: str, task_id: str) -> dict:

    task, task_instance = create_task_instance(dag_id, task_id)

    return task_instance.get_template_context()
