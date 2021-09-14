from airflow.models import DagBag, TaskInstance
from datetime import datetime, date
from dags.database import create_db_context, query_all_elements

from dags.nuts_regions_population.nuts_regions import REGIONS_ARGS
from dags.nuts_regions_population.population import POPULATION_ARGS
from dags.nuts_regions_population.german_counties_more_info import COUNTIES_ARGS


def test_nuts_regions_population_tasks_executes(db_context):
    run_task_with_url(REGIONS_TASK, REGIONS_URL)

    [_, SCHEMA, TABLE] = REGIONS_ARGS
    db_context = create_db_context()
    db_entries = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE};")
    assert len(db_entries) > 10
    assert db_entries[0] == (0, "BE", "Belgique/België", 1)


def test_population_task_executes_after_regions_task(db_context):
    run_task_with_url(REGIONS_TASK, REGIONS_URL)
    run_task_with_url(POPULATION_TASK, POPULATION_URL)

    [_, SCHEMA, TABLE] = POPULATION_ARGS
    db_context = create_db_context()

    db_entries = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE};")

    assert db_entries[0] == (1, 5841215, "BE", "TOTAL", "F", 2020, "")
    assert len(db_entries) == 1130


def test_counties_task_executes_after_regions_task(db_context):
    run_task_with_url(REGIONS_TASK, REGIONS_URL)
    run_task_with_url(COUNTIES_TASK, COUNTIES_URL)

    [_, SCHEMA, TABLE] = COUNTIES_ARGS
    db_context = create_db_context()

    db_entries = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE};")

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


def run_task_with_url(task: str, url: str):
    dag = DagBag().get_dag("nuts_regions_population")
    task0 = dag.get_task(task)
    task0.op_args[0] = url
    execution_date = datetime.now()
    task0instance = TaskInstance(task=task0, execution_date=execution_date)

    task0instance.get_template_context()
    task0.prepare_for_execution().execute(task0instance.get_template_context())
