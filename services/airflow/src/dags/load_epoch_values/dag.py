from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from src.dags.load_epoch_values import extract_load_epoch_data
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)

default_args = {
    "owner": "jakob",
    "depends_on_past": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}


dag = DAG(
    "load_epoch_to_clickhouse",
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to clickhouse",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 10, 1, 22),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="load_epoch_vitals",
    python_callable=extract_load_epoch_data,
    dag=dag,
)
