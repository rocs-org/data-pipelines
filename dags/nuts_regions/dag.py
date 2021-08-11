from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import ramda as R
from returns.curry import curry

from dags.nuts_regions.download_nuts_regions import download_nuts_regions
from dags.nuts_regions.transform_nuts_regions import transform_nuts_regions
from dags.nuts_regions.upload_to_postgres import upload_to_postgres
from dags.helpers.test_helpers.helpers import (
    insert_url_from_dag_conf,
    set_env_variable_from_dag_config_if_present,
)

URL = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx"

SCHEMA = "censusdata"
TABLE = "nuts"

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
    "nuts_regions",
    default_args=default_args,
    description="Load NUTS 2021 regions",
    start_date=days_ago(2),
    tags=["example"],
)


@curry
def task(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_nuts_regions(url),
        transform_nuts_regions,
        upload_to_postgres(schema, table),
    )(kwargs)


t1 = PythonOperator(
    task_id="extract",
    python_callable=insert_url_from_dag_conf(task),
    dag=dag,
    op_args=[URL, SCHEMA, TABLE],
)
