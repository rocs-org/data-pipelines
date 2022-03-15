from typing import Callable, List
from pandas.core.frame import DataFrame
from returns.pipeline import pipe
import ramda as R
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from returns.curry import curry

from postgres_helpers import (
    DBContext,
    execute_values,
)
from src.lib.dag_helpers import (
    download_csv,
    connect_to_db_and_insert_pandas_dataframe,
)
from src.lib.test_helpers import (
    set_env_variable_from_dag_config_if_present,
    if_var_exists_in_dag_conf_use_as_first_arg,
)

URL = "https://drive.google.com/uc?export=download&id=1t_WFejY2lXj00Qkc-6RAFgyr4sm5woQz"


@curry
def download_csv_and_upload_to_postgres(url: str, table: str, **kwargs) -> DBContext:
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_csv(url),
        transform_data({"some": "parameters"}),
        connect_to_db_and_insert_pandas_dataframe("test_tables", table),
        R.prop("credentials"),
    )(kwargs)


@R.curry
def transform_data(config: dict, data: DataFrame) -> DataFrame:
    # Transform data here for simple etl tasks or
    # connect to db via database.create_db_context() and do more elaborate things that also depend
    # on data that is already in the db. For stability however, it is adviced to make sure that the tasks that
    # are to provide the data have already run (e.g. with airflows ExternalTaskSensor class)
    print(config)
    return data


@R.curry
def write_dataframe_to_postgres(context: DBContext, table: str, data: DataFrame):
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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
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
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", download_csv_and_upload_to_postgres
    ),
    dag=dag,
    op_args=[URL, "test_table"],
)
