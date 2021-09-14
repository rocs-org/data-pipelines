from dags.database import DBContext, query_all_elements, create_db_context

from .etl import population_task, POPULATION_ARGS

from dags.nuts_regions_population.nuts_regions import regions_task, REGIONS_ARGS

[_, SCHEMA, TABLE] = POPULATION_ARGS
URL = "http://static-files/static/demo_r_pjangrp3.tsv"

[_, REGIONS_SCHEMA, REGIONS_TABLE] = REGIONS_ARGS
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"


def test_population_task_writes_to_db(db_context: DBContext):

    regions_task(REGIONS_URL, REGIONS_SCHEMA, REGIONS_TABLE)
    population_task(URL, SCHEMA, TABLE)

    db_context = create_db_context()
    db_entries = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE}")

    assert len(db_entries) == 1130
    assert db_entries[0] == (1, 5841215, "BE", "TOTAL", "F", 2020, "")