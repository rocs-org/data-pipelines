import ramda as R
from returns.curry import curry

from src.dags.nuts_regions_population.nuts_regions.download_nuts_regions import (
    download_nuts_regions,
)
from src.dags.nuts_regions_population.nuts_regions.transform_nuts_regions import (
    transform_nuts_regions,
)
from src.lib.dag_helpers.write_dataframe_to_postgres import (
    connect_to_db_and_insert_pandas_dataframe,
)
from src.lib.test_helpers import (
    set_env_variable_from_dag_config_if_present,
)

URL = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx"

SCHEMA = "censusdata"
TABLE = "nuts"


REGIONS_ARGS = [URL, SCHEMA, TABLE]


@curry
def etl_eu_regions(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_nuts_regions(url),
        transform_nuts_regions,
        connect_to_db_and_insert_pandas_dataframe(schema, table),
    )(kwargs)
