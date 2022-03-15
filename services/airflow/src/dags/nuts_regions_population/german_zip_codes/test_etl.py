from .etl import etl_german_zip_codes, ZIP_ARGS
from postgres_helpers import DBContext, query_all_elements
from src.dags.nuts_regions_population.nuts_regions import etl_eu_regions, REGIONS_ARGS
from src.dags.corona_cases.incidences.test_incidences import (
    replace_url_in_args_and_run_task,
)

[_, SCHEMA, TABLE] = ZIP_ARGS


def test_zip_codes_etl_writes_correct_values_to_db(pg_context: DBContext):

    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, etl_eu_regions)

    replace_url_in_args_and_run_task(ZIP_URL, ZIP_ARGS, etl_german_zip_codes)

    zips_from_db = query_all_elements(pg_context, f"SELECT * FROM {SCHEMA}.{TABLE};")

    assert len(zips_from_db) == 8181  # the number of zips for places in germany
    assert zips_from_db[0] == ("70173", "DE111")


REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
ZIP_URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"
