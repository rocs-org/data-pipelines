import ramda as R
from returns.curry import curry

from dags.nuts_regions_population.nuts_regions.download_nuts_regions import (
    download_nuts_regions,
)
from dags.nuts_regions_population.nuts_regions.transform_nuts_regions import (
    transform_nuts_regions,
)
from dags.helpers.dag_helpers.write_dataframe_to_postgres import (
    connect_to_db_and_insert,
)
from dags.helpers.test_helpers import (
    set_env_variable_from_dag_config_if_present,
)

URL = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx"

SCHEMA = "censusdata"
TABLE = "nuts"


REGIONS_ARGS = [URL, SCHEMA, TABLE]


@curry
def regions_task(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        R.tap(lambda *x: print("download regions data")),
        lambda *args: download_nuts_regions(url),
        R.tap(lambda *x: print("transform regions data")),
        transform_nuts_regions,
        R.tap(lambda *x: print("upload regions data")),
        connect_to_db_and_insert(schema, table),
        R.tap(lambda *x: print("done")),
    )(kwargs)
