from datetime import timedelta
from pendulum import today

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.dags.datenspende_vitaldata.data_update import (
    vital_data_update_etl,
    VITAL_DATA_UPDATE_ARGS,
)
from src.dags.datenspende_vitaldata.post_processing import (
    pivot_vitaldata,
    PIVOT_TARGETS,
)
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
    create_dbt_task_tree,
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
    "datenspende_vitaldata_v2",
    default_args=default_args,
    description="ETL vital data from thryve",
    schedule=timedelta(days=1),
    start_date=today("UTC").add(days=-1),
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

t1 >> t2

dbt_tasks = create_dbt_task_tree(
    dag=dag,
    base_task=t1,
    dbt_dir="/opt/airflow/dbt/",
    dbt_verb="run",
    env={
        "TARGET_DB_SCHEMA": "datenspende_derivatives",
        "DBT_LOGS": "/opt/airflow/logs/dbt/",
    },
)

# Since the thryve data loading is disabled, the above is merely a proof of concept.
# The following is the actual DAG:

dag2 = DAG(
    "data_donation_in_warehouse",
    default_args=default_args,
    description="Data donation in warehouse transformations",
    schedule=timedelta(days=1),
    start_date=today("UTC").add(days=-1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)


create_dbt_task_tree(
    dag=dag2,
    base_task=None,
    dbt_dir="/opt/airflow/dbt/",
    dbt_verb="run",
    env={
        "TARGET_DB_SCHEMA": "datenspende_derivatives",
        "DBT_LOGS": "/opt/airflow/logs/dbt/",
    },
)
