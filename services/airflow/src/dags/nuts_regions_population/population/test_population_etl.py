from database import (
    DBContext,
    query_all_elements,
)

from .etl import etl_population, POPULATION_ARGS

from src.dags.nuts_regions_population.nuts_regions import (
    etl_eu_regions,
    REGIONS_ARGS,
)

[_, SCHEMA, TABLE] = POPULATION_ARGS
URL = "http://static-files/static/demo_r_pjangrp3.tsv"

[_, REGIONS_SCHEMA, REGIONS_TABLE] = REGIONS_ARGS
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"


def test_population_task_writes_to_db(pg_context: DBContext):

    etl_eu_regions(REGIONS_URL, REGIONS_SCHEMA, REGIONS_TABLE)
    etl_population(URL, SCHEMA, TABLE)

    db_entries = query_all_elements(pg_context, f"SELECT * FROM {SCHEMA}.{TABLE}")

    assert len(db_entries) == 1130
    assert db_entries[0] == (1, 5841215, "BE", "TOTAL", "F", 2020, "")
