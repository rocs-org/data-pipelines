from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.dags.datenspende_vitaldata.data_update import (
    vital_data_update_etl,
    VITAL_DATA_UPDATE_ARGS,
)
from src.dags.datenspende_vitaldata.post_processing import (
    pivot_vitaldata,
    PIVOT_TARGETS,
    BEFORE_INFECTION_AGG_DB_PARAMETERS,
)
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)
from src.lib.dag_helpers import refresh_materialized_view
from src.lib.dag_helpers import run_dbt_models
from src.lib.test_helpers import if_var_exists_in_dag_conf_use_as_first_arg

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dvd.hnrchs@gmail.com"],
    "retries": 0,
    "retry": False,
    "provide_context": True,
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

t3 = PythonOperator(
    task_id="calculate_aggregate_per_user_statistics_of_daily_vitals",
    python_callable=refresh_materialized_view,
    op_args=[
        "datenspende_derivatives",
        "daily_vital_statistics",
    ],
    dag=dag,
)

t5 = PythonOperator(
    task_id="calculate_aggregate_vitals_per_user_source_and_type_before_first_infection",
    python_callable=refresh_materialized_view,
    op_args=BEFORE_INFECTION_AGG_DB_PARAMETERS,
)

t6 = PythonOperator(
    task_id="calculate_aggregates_vitals_per_source_type_and_date",
    python_callable=refresh_materialized_view,
    op_args=[
        "datenspende_derivatives",
        "aggregates_for_standardization_by_type_source_date",
    ],
)

t7 = PythonOperator(
    task_id="calculate_vitals_standardized_by_observation",
    python_callable=refresh_materialized_view,
    op_args=[
        "datenspende_derivatives",
        "vitals_standardized_by_daily_aggregates",
    ],
)

t8 = PythonOperator(
    task_id="calculate_aggregate_vitals_per_user_source_and_type_before_first_infection_from_vitals_aggregated_by",
    doc="""
        1) Standardize vitals grouped by date, source and type.
        2) Calculate aggregates over groups by user, source and type from vitals standardized by date,
           source and type for dates before first infection --  if a user was not infected, use all dates.
        """,
    python_callable=refresh_materialized_view,
    op_args=[
        "datenspende_derivatives",
        "vital_stats_before_infection_from_vitals_standardized_by_day",
    ],
)

t9 = PythonOperator(
    task_id="calculate_vitals_standardized_by_observation_and_user",
    doc="""
    1) Standardize vitals grouped by date, source and type to correct for seasonal variation
    2) Standardize vitals grouped by user, source and type before infection to account for individually varying baseline
    """,
    python_callable=refresh_materialized_view,
    op_args=[
        "datenspende_derivatives",
        "vitals_standardized_by_date_and_user_before_infection",
    ],
)

t10 = PythonOperator(
    task_id="run_dbt_models",
    doc="""
    1) Run the dbt models selected in the op_args against the specified target schema
    """,
    python_callable=run_dbt_models,
    op_args=[
        "datenspende",
        "datenspende_derivatives",
    ],
)

t1 >> [t2, t3, t5, t6, t10]
t6 >> t7 >> t8 >> t9
