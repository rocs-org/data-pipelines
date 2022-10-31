from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import today

from src.dags.datenspende_epoch_data.data_update import (
    el_epoch_data,
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
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "datenspende_epoch_data_import_v1",
    default_args=default_args,
    description="ETL vital data from thryve",
    schedule=timedelta(days=1),
    start_date=today("UTC").add(days=-1, hours=-2),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_epoch_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg("URL", el_epoch_data),
    dag=dag,
    op_args=VITAL_DATA_UPDATE_ARGS,
)
