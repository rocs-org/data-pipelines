import pandas as pd
from database import DBContext, create_db_context, teardown_test_db_context
from dags.update_hospitalizations.etl_hospitalizations import (
    etl_hospitalizations,
    HOSPITALIZATIONS_ARGS,
)

from dags.helpers.test_helpers import run_task_with_url

[_, HOSPITALIZATIONS_SCHEMA, HOSPITALIZATIONS_TABLE] = HOSPITALIZATIONS_ARGS
HOSPITALIZATIONS_URL = "http://static-files/static/hospitalizations.csv"


def test_etl_hospitalizations(db_context: DBContext):

    etl_hospitalizations(
        HOSPITALIZATIONS_URL, HOSPITALIZATIONS_SCHEMA, HOSPITALIZATIONS_TABLE
    )

    context = create_db_context()

    hospitalizations = pd.read_sql(
        f"SELECT * FROM {HOSPITALIZATIONS_SCHEMA}.{HOSPITALIZATIONS_TABLE}",
        context["connection"],
    )

    assert len(hospitalizations) == 82


def test_etl_runs_in_dag(db_context: DBContext):

    run_task_with_url(
        "update_hospitalizations", "load_hospitalizations", HOSPITALIZATIONS_URL
    )

    context = create_db_context()

    hospitalizations = pd.read_sql(
        f"SELECT * FROM {HOSPITALIZATIONS_SCHEMA}.{HOSPITALIZATIONS_TABLE}",
        context["connection"],
    )

    assert len(hospitalizations) == 82
