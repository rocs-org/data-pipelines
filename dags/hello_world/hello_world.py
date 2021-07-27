from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.airflow_fp import pull_execute_push, pull_execute, execute_push


def extract() -> str:
    return "hello_world"


def transform(data: str) -> str:
    return data.upper()


def load(data: str) -> str:
    print(data)
    return data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}

dag = DAG(
    "hello_world_etl",
    default_args=default_args,
    description="A hello world DAG with dumy extract, transform and load tasks",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
)

t1 = PythonOperator(
    task_id="extract", python_callable=execute_push("data1", extract), dag=dag
)
t2 = PythonOperator(
    task_id="transform",
    python_callable=pull_execute_push("data1", "data2", transform),
    dag=dag,
)
t3 = PythonOperator(
    task_id="load", python_callable=pull_execute("data2", transform), dag=dag
)
t1 >> t2 >> t3
