from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

from dags.datenspende_vitaldata.data_update import data_update_etl, DATA_UPDATE_ARGS
from dags.helpers.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)
from dags.helpers.test_helpers import if_var_exists_in_dag_conf_use_as_first_arg


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dvd.hnrchs@gmail.com"],
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "datenspende-vitaldata",
    default_args=default_args,
    description="ETL vital data from thryve",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_vital_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg("URL", data_update_etl),
    dag=dag,
    op_args=DATA_UPDATE_ARGS,
)
