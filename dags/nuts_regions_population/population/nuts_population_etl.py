import ramda as R
from returns.curry import curry

from dags.helpers.dag_helpers import connect_to_db_and_insert
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present
from dags.nuts_regions_population.population.download_population_data import (
    download_data,
)
from dags.nuts_regions_population.population.transform_population_data import transform

URL = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/demo_r_pjangrp3.tsv.gz"

SCHEMA = "censusdata"
TABLE = "population"


POPULATION_ARGS = [URL, SCHEMA, TABLE]


@curry
def population_task(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_data(url),
        transform,
        connect_to_db_and_insert(schema, table),
    )(kwargs)
