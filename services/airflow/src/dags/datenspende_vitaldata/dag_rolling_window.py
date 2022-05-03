from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

from src.dags.datenspende_vitaldata.post_processing import (
    rolling_window_time_series_features_pipeline,
)

from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dvd.hnrchs@gmail.com"],
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "datenspende_vitaldata_rolling_window",
    default_args=default_args,
    description="rolling window features of vital data",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1, hour=22),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t4 = PythonOperator(
    task_id="calculate_rolling_window_statistics_of_daily_vitals",
    python_callable=rolling_window_time_series_features_pipeline,
    dag=dag,
)
