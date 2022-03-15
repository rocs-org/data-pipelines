from datetime import datetime
from postgres_helpers import DBContext, query_all_elements
from airflow.models import DagBag

from src.lib.test_helpers import execute_dag
from src.dags.corona_cases.cases import CASES_ARGS
from src.dags.corona_cases.incidences import INCIDENCES_ARGS


[_, CASES_SCHEMA, CASES_TABLE] = CASES_ARGS
[INCIDENCES_SCHEMA, INCIDENCES_TABLE] = INCIDENCES_ARGS


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_corona_cases_dag_writes_correct_results_to_db(pg_context: DBContext):
    credentials = pg_context["credentials"]

    # run population data pipeline first, as incidence calculation depends on its results

    assert (
        execute_dag(
            "nuts_regions_population",
            "2021-01-01",
            {
                "TARGET_DB": credentials["database"],
                "POPULATION_URL": POPULATION_URL,
                "REGIONS_URL": REGIONS_URL,
                "COUNTIES_URL": COUNTIES_URL,
                "ZIP_URL": ZIP_URL,
            },
        )
    ) == 0

    assert (
        execute_dag(
            "corona_cases",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": CASES_URL},
        )
        == 0
    )

    res_cases = query_all_elements(
        pg_context, f"SELECT * FROM {CASES_SCHEMA}.{CASES_TABLE}"
    )
    assert len(res_cases) == 9
    assert res_cases[0] == (
        1,
        "Schleswig-Holstein",
        1001,
        "SK Flensburg",
        "A35-A59",
        "M",
        datetime(2020, 10, 30, 0, 0),
        datetime(2020, 10, 27, 0, 0),
        True,
        0,
        -9,
        0,
        1,
        0,
        1,
    )

    res_incidence = query_all_elements(
        pg_context, f"SELECT * FROM {INCIDENCES_SCHEMA}.{INCIDENCES_TABLE};"
    )
    assert len(res_incidence) == 150
    assert res_incidence[8] == (
        1001,
        4,
        datetime(2020, 11, 7, 0, 0),
        0,
        0.0,
        0.0,
        0,
        "DEF01",
        90164.0,
        1.0,
    )


CASES_URL = "http://static-files/static/coronacases.csv"
POPULATION_URL = "http://static-files/static/demo_r_pjangrp3.tsv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/04-kreise.xlsx"
ZIP_URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"
