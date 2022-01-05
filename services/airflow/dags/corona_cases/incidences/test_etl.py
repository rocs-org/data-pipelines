from datetime import datetime
from database import (
    DBContext,
    query_all_elements,
    create_db_context,
    db_context,
    teardown_test_db_context,
    teardown_db_context,
)
from dags.corona_cases.incidences.test_incidences import (
    replace_url_in_args_and_run_task,
)

from dags.nuts_regions_population.nuts_regions import etl_eu_regions, REGIONS_ARGS
from dags.nuts_regions_population.german_counties_more_info import (
    etl_german_counties_more_info,
    COUNTIES_ARGS,
)
from dags.corona_cases.cases import etl_covid_cases, CASES_ARGS

from .etl import calculate_incidence_post_processing, INCIDENCES_ARGS


def test_incidences_etl_writes_incidences_to_db(db_context: DBContext):
    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, etl_eu_regions)
    replace_url_in_args_and_run_task(
        COUNTIES_URL, COUNTIES_ARGS, etl_german_counties_more_info
    )
    replace_url_in_args_and_run_task(CASES_URL, CASES_ARGS, etl_covid_cases)
    calculate_incidence_post_processing(*INCIDENCES_ARGS)
    [schema, table] = INCIDENCES_ARGS

    db_context = create_db_context()
    db_entries = query_all_elements(db_context, f"SELECT * FROM {schema}.{table}")
    teardown_db_context(db_context)
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
    calculate_incidence_post_processing(*INCIDENCES_ARGS)
    db_context = create_db_context()
    db_entries_after_dag_rerun = query_all_elements(
        db_context, f"SELECT * FROM {schema}.{table}"
    )
    teardown_db_context(db_context)

    assert len(db_entries_after_dag_rerun) == 150


CASES_URL = "http://static-files/static/coronacases.csv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/alle-kreise.xlsx"

if __name__ == "__main__":
    context = db_context()
    test_incidences_etl_writes_incidences_to_db(context)
    teardown_test_db_context(context)
