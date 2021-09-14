from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from dags.corona_cases.cases import etl_covid_cases, CASES_ARGS
from dags.corona_cases.incidences import (
    calculate_incidence_post_processing,
    INCIDENCES_ARGS,
)
from dags.helpers.test_helpers import (
    if_var_exists_in_dag_conf_use_as_first_arg,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "corona_cases",
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to postgres",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=["ROCS pipelines"],
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
