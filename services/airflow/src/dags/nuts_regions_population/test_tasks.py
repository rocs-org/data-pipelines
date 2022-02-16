from datetime import date
from database import query_all_elements
from src.lib.test_helpers.helpers import run_task_with_url

from src.dags.nuts_regions_population.nuts_regions import REGIONS_ARGS
from src.dags.nuts_regions_population.population import POPULATION_ARGS
from src.dags.nuts_regions_population.german_counties_more_info import (
    COUNTIES_ARGS,
)


def test_nuts_regions_population_tasks_executes(pg_context):
    run_task_with_url("nuts_regions_population", REGIONS_TASK, REGIONS_URL)

    [_, SCHEMA, TABLE] = REGIONS_ARGS
    db_entries = query_all_elements(pg_context, f"SELECT * FROM {SCHEMA}.{TABLE};")
    assert len(db_entries) > 10
    assert db_entries[0] == (0, "BE", "Belgique/België", 1)


def test_population_task_executes_after_regions_task(pg_context):
    run_task_with_url("nuts_regions_population", REGIONS_TASK, REGIONS_URL)
    run_task_with_url("nuts_regions_population", POPULATION_TASK, POPULATION_URL)

    [_, SCHEMA, TABLE] = POPULATION_ARGS

    db_entries = query_all_elements(pg_context, f"SELECT * FROM {SCHEMA}.{TABLE};")

    assert db_entries[0] == (1, 5841215, "BE", "TOTAL", "F", 2020, "")
    assert len(db_entries) == 1130


def test_counties_task_executes_after_regions_task(pg_context):
    run_task_with_url("nuts_regions_population", REGIONS_TASK, REGIONS_URL)
    run_task_with_url("nuts_regions_population", COUNTIES_TASK, COUNTIES_URL)

    [_, SCHEMA, TABLE] = COUNTIES_ARGS

    db_entries = query_all_elements(pg_context, f"SELECT * FROM {SCHEMA}.{TABLE};")

    assert db_entries[0] == (
        1,
        1001,
        "Kreisfreie Stadt",
        "Flensburg, Stadt",
        "DEF01",
        53.02,
        90164,
        44904,
        45260,
        1701.0,
        date(2019, 12, 31),
    )
    assert len(db_entries) == 15


REGIONS_TASK = "load_nuts_regions"
POPULATION_TASK = "load_population_for_nuts_regions"
COUNTIES_TASK = "load_more_info_on_german_counties"

POPULATION_URL = "http://static-files/static/demo_r_pjangrp3.tsv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/04-kreise.xlsx"
