from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from dags.nuts_regions_population.nuts_regions import (
    regions_task,
    REGIONS_ARGS,
)
from dags.nuts_regions_population.population import (
    population_task,
    POPULATION_ARGS,
)
from dags.nuts_regions_population.german_counties_more_info import (
    german_counties_more_info_etl,
    COUNTIES_ARGS,
)
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
    "nuts_regions_population",
    default_args=default_args,
    description="Load population data for NUTS 2021 regions from eurostat",
    start_date=days_ago(1),
    tags=["ROCS pipelines"],
)


t1 = PythonOperator(
    task_id="load_nuts_regions",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "REGIONS_URL", regions_task
    ),
    dag=dag,
    op_args=REGIONS_ARGS,
)

t2 = PythonOperator(
    task_id="load_more_info_on_german_counties",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "COUNTIES_URL", german_counties_more_info_etl
    ),
    dag=dag,
    op_args=COUNTIES_ARGS,
)

t3 = PythonOperator(
    task_id="load_population_for_nuts_regions",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "POPULATION_URL", population_task
    ),
    dag=dag,
    op_args=POPULATION_ARGS,
)


t1 >> t2 >> t3
