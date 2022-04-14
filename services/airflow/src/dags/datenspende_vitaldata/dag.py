from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

from src.dags.datenspende_vitaldata.data_update import (
    vital_data_update_etl,
    VITAL_DATA_UPDATE_ARGS,
)
from src.dags.datenspende_vitaldata.post_processing import (
    pivot_vitaldata,
    PIVOT_TARGETS,
    pipeline_for_aggregate_statistics_of_per_user_vitals,
    rolling_window_time_series_features_pipeline,
    BEFORE_INFECTION_AGG_DB_PARAMETERS,
    aggregate_statistics_before_infection,
)

from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)
from src.lib.test_helpers import if_var_exists_in_dag_conf_use_as_first_arg


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dvd.hnrchs@gmail.com"],
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "datenspende_vitaldata_v2",
    default_args=default_args,
    description="ETL vital data from thryve",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1, hour=2),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_vital_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", vital_data_update_etl
    ),
    dag=dag,
    op_args=VITAL_DATA_UPDATE_ARGS,
)

t2 = PythonOperator(
    task_id="create_vitaldata_pivot_tables",
    python_callable=pivot_vitaldata,
    dag=dag,
    op_args=PIVOT_TARGETS,
)

t3 = PythonOperator(
    task_id="calculate_aggregate_per_user_statistics_of_daily_vitals",
    python_callable=pipeline_for_aggregate_statistics_of_per_user_vitals,
    dag=dag,
)

t4 = PythonOperator(
    task_id="calculate_rolling_window_statistics_of_daily_vitals",
    python_callable=rolling_window_time_series_features_pipeline,
    dag=dag,
)

t5 = PythonOperator(
    task_id="calculate_aggregate_per_user_statistics_of_daily_vitals_before_first_infection",
    python_callable=aggregate_statistics_before_infection,
    op_args=BEFORE_INFECTION_AGG_DB_PARAMETERS,
)

t1 >> [t2, t3, t4, t5]
