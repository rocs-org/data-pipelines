import subprocess
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present, set_env_variable


def run_dbt_models(models: str, target_db_schema: str, **kwargs) -> None:
    """Run the specified dbt models with output in target_db_schema"""
    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    set_env_variable("DBT_LOGS", "/opt/airflow/logs/dbt/")
    set_env_variable("TARGET_DB_SCHEMA", target_db_schema)
    subprocess.run(
        [
            "dbt",
            "run",
            "--select",
            models,
            "--project-dir",
            "/opt/airflow/src/dbt/",
            "--profiles-dir",
            "/opt/airflow/src/dbt/"
        ],
    )
