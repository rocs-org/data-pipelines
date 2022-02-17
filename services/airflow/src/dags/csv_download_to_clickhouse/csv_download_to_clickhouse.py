from datetime import timedelta

import pandas as pd
import ramda as R
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from returns.curry import curry

from clickhouse_helpers import (
    DBCredentials,
    create_db_context,
    teardown_db_context,
    insert_dataframe,
)
from src.lib.dag_helpers import (
    download_csv,
)
from src.lib.test_helpers import (
    set_env_variable_from_dag_config_if_present,
    if_var_exists_in_dag_conf_use_as_first_arg,
)

URL = "https://drive.google.com/uc?export=download&id=1t_WFejY2lXj00Qkc-6RAFgyr4sm5woQz"


@curry
def download_csv_and_upload_to_clickhouse(
    url: str, table: str, **kwargs
) -> DBCredentials:
    return R.pipe(
        set_env_variable_from_dag_config_if_present("CLICKHOUSE_DB"),
        lambda *args: download_csv(url),
        load_df_to_clickhouse(table),
    )(kwargs)


@R.curry
def load_df_to_clickhouse(table: str, data: pd.DataFrame) -> DBCredentials:
    context = create_db_context()
    insert_dataframe(context, table, data.set_index("col1"))
    teardown_db_context(context)
    return context["credentials"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "extract_load_to_clickhouse",
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to clickhouse",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
)

t1 = PythonOperator(
    task_id="EL",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", download_csv_and_upload_to_clickhouse
    ),
    dag=dag,
    op_args=[URL, "test_table"],
)
