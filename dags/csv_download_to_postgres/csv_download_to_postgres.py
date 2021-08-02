from typing import Callable, List
from pandas.core.frame import DataFrame
import requests
import io
from pandas import read_csv
from returns.pipeline import pipe
import ramda as R
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from returns.curry import curry

from dags.database import (
    DB_Context,
    execute_values,
    create_db_context,
    teardown_db_context,
)
from dags.test_helpers.helpers import set_env_variable_from_dag_config_if_present

URL = "https://drive.google.com/uc?export=download&id=1t_WFejY2lXj00Qkc-6RAFgyr4sm5woQz"


@curry
def download_csv_and_upload_to_postgres(url: str, table: str, **kwargs) -> DB_Context:
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        create_db_context,
        R.tap(
            R.converge(
                write_dataframe_to_postgres,
                [R.identity, R.always(table), lambda *args: download_csv(url)],
            )
        ),
        teardown_db_context,
        R.path(["credentials", "database"]),
    )(kwargs)


def download_csv(url: str) -> DataFrame:
    return R.pipe(
        R.try_catch(
            requests.get,
            lambda err, url: _raise(
                FileNotFoundError(f"Cannot find file at {url} \n trace: \n {err}")
            ),
        ),
        R.prop("content"),
        R.invoker(1, "decode")("utf-8"),
        io.StringIO,
        read_csv,
    )(url)


@R.curry
def write_dataframe_to_postgres(context: DB_Context, table: str, data: DataFrame):
    return R.converge(execute_values(context), [_build_query(table), _get_tuples])(data)


@R.curry
def _build_query(table: str) -> Callable[[DataFrame], str]:
    return pipe(
        _get_columns,
        lambda columns: "INSERT INTO %s (%s) VALUES %%s;" % (table, columns),
    )


def _get_columns(df: DataFrame) -> str:
    return ",".join(list(df.columns))


def _get_tuples(df: DataFrame) -> List:
    return [tuple(x) for x in df.to_numpy()]


def _raise(ex: Exception):
    raise ex


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "example_csv_to_postgres",
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to postgres",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
)

t1 = PythonOperator(
    task_id="extract",
    python_callable=download_csv_and_upload_to_postgres,
    dag=dag,
    op_args=[URL, "test_table"],
)
