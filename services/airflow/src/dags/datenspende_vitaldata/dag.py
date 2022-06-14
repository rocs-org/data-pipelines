from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow_dbt.operators import DbtRunOperator

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
)
from src.lib.dag_helpers import run_dbt_models
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

t3 = BashOperator(dag=dag, task_id="where_am_i", bash_command="pwd")

t4 = PythonOperator(
    dag=dag,
    task_id="run_dbt_models",
    doc="""
    1) Run the dbt models selected in the op_args against the specified target schema
    """,
    python_callable=run_dbt_models,
    op_args=["datenspende", "datenspende_derivatives", "/opt/airflow/dbt/"],
)


t1 >> [t2, t3, t4]
