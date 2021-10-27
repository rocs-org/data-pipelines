from pandas.core.frame import DataFrame
import ramda as R
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from returns.curry import curry

from dags.database import (
    DBContext,
)
from dags.helpers.dag_helpers import (
    download_csv,
    connect_to_db_and_insert_pandas_dataframe,
)
from dags.helpers.test_helpers import (
    set_env_variable_from_dag_config_if_present,
    if_var_exists_in_dag_conf_use_as_first_arg,
)

URL = "https://drive.google.com/uc?export=download&id=1t_WFejY2lXj00Qkc-6RAFgyr4sm5woQz"


@curry
def download_csv_and_upload_to_postgres(url: str, table: str, **kwargs) -> DBContext:
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        # note that `download_csv(url)` is the return value of download_csv but
        # `lambda *args: download_csv(url)` is a function that returns the return value of `download_csv(url)`
        lambda *args: download_csv(url),
        transform_data({"some": "parameters"}),
        connect_to_db_and_insert_pandas_dataframe("test_tables", table),
        R.prop("credentials"),
    )(kwargs)


@R.curry
def transform_data(config: dict, data: DataFrame) -> DataFrame:
    # Transform data here for simple etl tasks or
    # connect to db via dags.database.create_db_context() and do more elaborate things that also depend
    # on data that is already in the db. For stability however, it is adviced to make sure that the tasks that
    # are to provide the data have already run (e.g. with airflows ExternalTaskSensor class)
    print(config)
    return data


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
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", download_csv_and_upload_to_postgres
    ),
    dag=dag,
    op_args=[URL, "test_table"],
)
