from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from dags.corona_cases.cases import covid_cases_etl, CASES_ARGS
from dags.helpers.test_helpers.helpers import (
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
    start_date=days_ago(2),
    tags=["ROCS pipelines"],
)

t1 = PythonOperator(
    task_id="extract",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg("URL", covid_cases_etl),
    dag=dag,
    op_args=CASES_ARGS,
)
