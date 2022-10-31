from airflow import DAG
from pendulum import today
from airflow.operators.python import PythonOperator

from src.dags.update_hospitalizations.etl_hospitalizations import (
    etl_hospitalizations,
    HOSPITALIZATIONS_ARGS,
)
from src.lib.test_helpers.helpers import (
    if_var_exists_in_dag_conf_use_as_first_arg,
)
from src.lib.dag_helpers import (
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "update_hospitalizations",
    default_args=default_args,
    description="Load icu admission rate hospitalization data from opendata",
    start_date=today("UTC").add(days=-1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)


t1 = PythonOperator(
    task_id="load_hospitalizations",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "URL", etl_hospitalizations
    ),
    dag=dag,
    op_args=HOSPITALIZATIONS_ARGS,
)

t1
