from datetime import date

from airflow.models import DagBag

from postgres_helpers import DBContext, query_all_elements
from src.dags.update_hospitalizations.dag import HOSPITALIZATIONS_ARGS
from src.lib.test_helpers import execute_dag
from src.lib.test_helpers import run_task_with_url

[_, HOSPITALIZATIONS_SCHEMA, HOSPITALIZATIONS_TABLE] = HOSPITALIZATIONS_ARGS


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_writes_correct_results_to_db(pg_context: DBContext):
    run_task_with_url(
        "nuts_regions_population",
        "load_nuts_regions",
        "http://static-files/static/NUTS2021.xlsx",
    )
    run_task_with_url(
        "nuts_regions_population",
        "load_population_for_nuts_regions",
        "http://static-files/static/demo_r_pjangrp3.tsv",
    )

    credentials = pg_context["credentials"]

    assert (
        execute_dag(
            "update_hospitalizations",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": HOSPITALIZATIONS_URL},
        )
        == 0
    )
    res_cases = query_all_elements(
        pg_context, f"SELECT * FROM {HOSPITALIZATIONS_SCHEMA}.{HOSPITALIZATIONS_TABLE}"
    )
    assert len(res_cases) == 82
    test_case_tuple = res_cases[0]
    test_case_trunc = test_case_tuple[:1] + test_case_tuple[2:]
    assert test_case_trunc == (
        1,
        2230,
        2.68136129610801,
        2020,
        12,
        "2020-W12",
        date(2020, 3, 16),
        date(2020, 3, 22),
        75,
        81,
    )


HOSPITALIZATIONS_URL = "http://static-files/static/hospitalizations.csv"
