from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from pendulum import today
from src.dags.corona_cases.cases import etl_covid_cases, CASES_ARGS
from src.dags.corona_cases.incidences import (
    calculate_incidence_post_processing,
    INCIDENCES_ARGS,
)
from src.lib.dag_helpers import (
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)
from src.lib.test_helpers import (
    if_var_exists_in_dag_conf_use_as_first_arg,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "corona_cases",
    default_args=default_args,
    description="Download covid cases and calculate regional 7 day incidence values.",
    schedule=timedelta(days=1),
    start_date=today("UTC").add(days=-1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_cases_data",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg("URL", etl_covid_cases),
    dag=dag,
    op_args=CASES_ARGS,
)

wait_for_demographics_data = ExternalTaskSensor(
    task_id="wait_for_demographics_data",
    external_dag_id="nuts_regions_population",
    external_task_id="load_more_info_on_german_counties",
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
)

t2 = PythonOperator(
    task_id="calculate_incidences",
    python_callable=calculate_incidence_post_processing,
    dag=dag,
    op_args=INCIDENCES_ARGS,
)

t1 >> wait_for_demographics_data >> t2
