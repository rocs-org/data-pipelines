import os
import ramda as R
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago


from dags.helpers.test_helpers.helpers import (
    check_if_var_exists_in_dag_conf,
    set_env_variable,
    set_env_variable_from_dag_config_if_present,
    run_task_with_url,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "provide_context": True,
}
dag_id = "dag_name"
task_id = "task_name"

dag = DAG(
    dag_id,
    default_args=default_args,
    start_date=days_ago(2),
    tags=["testing"],
)
PythonOperator(
    task_id=task_id,
    python_callable=R.tap(print),
    dag=dag,
    op_args=["url1"],
)


def test_run_task_with_url_works_without_error():

    res = run_task_with_url(dag_id=dag_id, task_id=task_id, url="url2")

    assert res == "url2"


def test_check_if_var_exists_in_dag_conf():
    assert (
        check_if_var_exists_in_dag_conf("var", {"dag_run": {"conf": {"var": "value"}}})
        is True
    )
    assert (
        check_if_var_exists_in_dag_conf(
            "varrr", {"dag_run": {"conf": {"var": "value"}}}
        )
        is False
    )
    assert check_if_var_exists_in_dag_conf("var", {"dag_run": {}}) is False
    assert check_if_var_exists_in_dag_conf("var", {}) is False


def test_set_env_variable():
    set_env_variable("abc", "value")
    assert os.environ.get("abc") == "value"


def test_set_env_variable_from_dag_config():
    set_env_variable_from_dag_config_if_present("var")(
        {"dag_run": {"conf": {"var": "value"}}}
    )
    assert os.environ.get("var") == "value"
