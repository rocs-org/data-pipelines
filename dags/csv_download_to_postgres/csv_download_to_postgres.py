from dags.database.db_context import create_test_db_context, pipe0
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

try:
    from dags.database import (
        DB_Context,
        execute_values,
        create_db_context,
        teardown_db_context,
    )
except ModuleNotFoundError:
    from database import DB_Context, execute_values  # type: ignore

URL = "https://shitcloud.hopto.org/s/JoM6bZiQ24gRze2/download/test.csv"


@R.curry
def download_csv_and_upload_to_postgres(url: str, table: str) -> DB_Context:
    return pipe0(
        create_db_context,
        R.tap(print),
        R.tap(
            R.converge(
                write_dataframe_to_postgres,
                [R.identity, R.always(table), lambda *args: download_csv(url)],
            )
        ),
        teardown_db_context,
        R.path(["credentials", "database"]),
    )()


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


def _get_columns(df: DataFrame) -> str:
    return ",".join(list(df.columns))


def _get_tuples(df: DataFrame) -> List:
    return [tuple(x) for x in df.to_numpy()]


@R.curry
def write_dataframe_to_postgres(context: DB_Context, table: str, data: DataFrame):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Comma-separated dataframe columns
    # SQL quert to execute
    return R.converge(execute_values(context), [_build_query(table), _get_tuples])(data)


def _raise(ex: Exception):
    raise ex


@R.curry
def _build_query(table: str) -> Callable[[DataFrame], str]:
    return pipe(
        _get_columns,
        lambda columns: "INSERT INTO %s (%s) VALUES %%s;" % (table, columns),
    )


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
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(2),
    tags=["example"],
)

t1 = PythonOperator(
    task_id="extract",
    python_callable=download_csv_and_upload_to_postgres,
    dag=dag,
    op_args=[URL, "test_table"],
)

t2 = PythonOperator(
    task_id="transform",
    python_callable=lambda: print("that worked!"),
    dag=dag,
)

t1 >> t2
