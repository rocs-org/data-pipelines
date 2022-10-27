from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.dags.datenspende_vitaldata_onetime_update.onetime_data_update_task import (
    ONETIME_VITAL_DATA_UPDATE_ARGS,
)
from src.dags.datenspende_vitaldata.data_update.data_update_task import (
    vital_data_update_etl,
)
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)
from src.lib.test_helpers import if_var_exists_in_dag_conf_use_as_first_arg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
    "dir": "/opt/airflow/dbt/",
}

dag = DAG(
    "datenspende_vitaldata_onetime_update",
    default_args=default_args,
    description="Onetime ETL vital data from thryve for data fixes",
    schedule_interval=None,
    start_date=days_ago(1, hour=2),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_onetime_vital_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", vital_data_update_etl
    ),
    dag=dag,
    op_args=ONETIME_VITAL_DATA_UPDATE_ARGS,
)

t1
