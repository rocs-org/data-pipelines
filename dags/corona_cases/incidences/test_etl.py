from datetime import datetime
from dags.database import DBContext, query_all_elements, create_db_context
from .test_incidences import replace_url_in_args_and_run_task

from dags.nuts_regions_population.nuts_regions import regions_task, REGIONS_ARGS
from dags.nuts_regions_population.german_counties_more_info import (
    german_counties_more_info_etl,
    COUNTIES_ARGS,
)
from dags.corona_cases.cases import covid_cases_etl, CASES_ARGS

from .etl import incidences_etl, INCIDENCES_ARGS


def test_incidences_etl_writes_incidences_to_db(db_context: DBContext):

    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, regions_task)
    replace_url_in_args_and_run_task(
        COUNTIES_URL, COUNTIES_ARGS, german_counties_more_info_etl
    )
    replace_url_in_args_and_run_task(CASES_URL, CASES_ARGS, covid_cases_etl)

    incidences_etl(*INCIDENCES_ARGS)

    [schema, table] = INCIDENCES_ARGS

    db_context = create_db_context()
    db_entries = query_all_elements(db_context, f"SELECT * FROM {schema}.{table}")

    assert len(db_entries) == 150
    assert db_entries[8] == (
        1001,
        4,
        datetime(2020, 11, 7, 0, 0),
        0,
        0.0,
        0.0,
        0,
        "DEF01",
        89934.0,
        1.0,
    )


CASES_URL = "http://static-files/static/coronacases.csv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/alle-kreise.xlsx"
