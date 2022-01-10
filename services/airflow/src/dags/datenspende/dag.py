from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from src.dags.datenspende.data_update import data_update_etl, DATA_UPDATE_ARGS
from src.dags.datenspende.answers_post_processing import (
    post_processing_test_and_symptoms_answers,
)
from src.dags.datenspende.case_detection_features import (
    extract_features_task,
    WEEKLY_FEATURE_EXTRACTION_ARGS,
    ONE_OFF_FEATURE_EXTRACTION_ARGS,
)
from src.lib.dag_helpers import (
    create_slack_error_message_from_task_context,
    slack_notifier_factory,
)
from src.lib.test_helpers import if_var_exists_in_dag_conf_use_as_first_arg


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "datenspende_surveys_v2",
    default_args=default_args,
    description="ETL study data from thryve",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1, hour=1),
    tags=["ROCS pipelines"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
)

t1 = PythonOperator(
    task_id="gather_data_from_thryve",
    python_callable=if_var_exists_in_dag_conf_use_as_first_arg("URL", data_update_etl),
    dag=dag,
    op_args=DATA_UPDATE_ARGS,
)

t2 = PythonOperator(
    task_id="post_process_test_and_symptoms_answers",
    python_callable=post_processing_test_and_symptoms_answers,
    dag=dag,
)

t3 = PythonOperator(
    task_id="extract_features_from_weekly_tasks",
    python_callable=extract_features_task,
    dag=dag,
    op_args=WEEKLY_FEATURE_EXTRACTION_ARGS,
)

t4 = PythonOperator(
    task_id="extract_features_from_one_off_answers",
    python_callable=extract_features_task,
    dag=dag,
    op_args=ONE_OFF_FEATURE_EXTRACTION_ARGS,
)

t1 >> [t2, t3, t4]
