from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from src.dags.nuts_regions_population.nuts_regions import (
    etl_eu_regions,
    REGIONS_ARGS,
)
from src.dags.nuts_regions_population.population import (
    etl_population,
    POPULATION_ARGS,
)
from src.dags.nuts_regions_population.german_counties_more_info import (
    etl_german_counties_more_info,
    COUNTIES_ARGS,
)
from src.dags.nuts_regions_population.german_zip_codes import (
    etl_german_zip_codes,
    ZIP_ARGS,
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
    "nuts_regions_population",
    default_args=default_args,
    description="Load population data for NUTS 2021 regions from eurostat",
    start_date=days_ago(1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)


t1 = PythonOperator(
    task_id="load_nuts_regions",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "REGIONS_URL", etl_eu_regions
    ),
    dag=dag,
    op_args=REGIONS_ARGS,
)

t2 = PythonOperator(
    task_id="load_more_info_on_german_counties",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "COUNTIES_URL", etl_german_counties_more_info
    ),
    dag=dag,
    op_args=COUNTIES_ARGS,
)

t3 = PythonOperator(
    task_id="load_population_for_nuts_regions",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "POPULATION_URL", etl_population
    ),
    dag=dag,
    op_args=POPULATION_ARGS,
)

t4 = PythonOperator(
    task_id="load_german_zip_codes",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg(
        "ZIP_URL", etl_german_zip_codes
    ),
    dag=dag,
    op_args=ZIP_ARGS,
)


t1 >> t2 >> t3 >> t4
