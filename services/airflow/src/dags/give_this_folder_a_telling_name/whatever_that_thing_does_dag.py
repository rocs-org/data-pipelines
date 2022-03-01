import pandas as pd
import ramda as R
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from returns.curry import curry

from database import DBContext, create_db_context, teardown_db_context
from src.lib.dag_helpers import (
    connect_to_db_and_insert_pandas_dataframe,
)
from src.lib.test_helpers import (
    set_env_variable_from_dag_config_if_present,
)
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)

TABLE = "your_new_table"
SCHEMA = "your_new_schema"


@curry
def download_csv_and_upload_to_postgres(schema: str, table: str, **kwargs) -> DBContext:
    # we want to write to the test database during tests
    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)

    # load some data from the db
    data = execute_query_and_return_dataframe(
        "SELECT stuff FROM some_schema.some_table"
    )

    # Do whatever to create your data as a pandas dataframe

    df = some_function_with_tests(data)

    # Write it to the db
    connect_to_db_and_insert_pandas_dataframe(schema, table, df),


def some_function_with_tests(data: pd.DataFrame) -> pd.DataFrame:
    return data


@R.curry
def execute_query_and_return_dataframe(query: str):
    context = create_db_context()
    data = pd.read_sql(query, con=context["connection"])
    teardown_db_context(context)
    return data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "annikas_data_processing_pipeline",
    default_args=default_args,
    description="A more telling description of what is going on",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="the_script_name",
    python_callable=download_csv_and_upload_to_postgres,
    dag=dag,
    op_args=[SCHEMA, TABLE],
)
