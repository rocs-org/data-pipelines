import subprocess
from src.lib.test_helpers import (
    set_env_variable_from_dag_config_if_present,
    set_env_variable,
)
import logging
from airflow.exceptions import AirflowException

log = logging.getLogger()


def run_dbt_models(models: str, target_db_schema: str, dir: str, **kwargs) -> None:
    """Run the specified dbt models with output in target_db_schema"""

    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    set_env_variable("DBT_LOGS", "/opt/airflow/logs/dbt/")
    set_env_variable("TARGET_DB_SCHEMA", target_db_schema)
    sp = subprocess.Popen(
        [
            "dbt",
            "run",
            "--select",
            models,
            "--project-dir",
            "/opt/airflow/dbt/",
            "--profiles-dir",
            "/opt/airflow/dbt/",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=dir,
        close_fds=True,
    )
    log.info("Output:")
    for line in iter(sp.stdout.readline, b""):
        line = line.decode("utf-8").rstrip()
        log.info(line)
    sp.wait()
    log.info("Command exited with return code %s", sp.returncode)

    if sp.returncode:
        raise AirflowException("dbt command failed")
