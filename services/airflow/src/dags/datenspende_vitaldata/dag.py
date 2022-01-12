from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

from src.dags.datenspende_vitaldata.data_update import (
    vital_data_update_etl,
    VITAL_DATA_UPDATE_ARGS,
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
    start_date=days_ago(1, hour=1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

externalsensor1 = ExternalTaskSensor(
    task_id="dag_datenspende_completed_status",
    external_dag_id="datenspende_surveys_v2",
    external_task_id="gather_data_from_thryve",
    check_existence=True,
    timeout=1200,
)

t1 = PythonOperator(
    task_id="gather_vital_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", vital_data_update_etl
    ),
    dag=dag,
    op_args=VITAL_DATA_UPDATE_ARGS,
)

externalsensor1 >> t1