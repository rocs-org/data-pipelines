from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from dags.corona_cases.download_corona_cases import download_csv_and_upload_to_postgres
from dags.helpers.test_helpers.helpers import (
    insert_url_from_dag_conf,
)

URL = "https://prod-hub-indexer.s3.amazonaws.com/files/dd4580c810204019a7b8eb3e0b329dd6/0/full/4326/dd4580c810204019a7b8eb3e0b329dd6_0_full_4326.csv"  # noqa: E501

SCHEMA = "coronacases"
TABLE = "german_counties_more_info"

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
    tags=["example"],
)

t1 = PythonOperator(
    task_id="extract",
    python_callable=insert_url_from_dag_conf(download_csv_and_upload_to_postgres),
    dag=dag,
    op_args=[URL, SCHEMA, TABLE],
)
