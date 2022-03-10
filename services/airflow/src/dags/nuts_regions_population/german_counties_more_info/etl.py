import ramda as R
from returns.curry import curry
from .download import download
from .transform import transform
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from src.lib.dag_helpers import connect_to_db_and_insert_pandas_dataframe


URL = (
    "https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales"
    "/Gemeindeverzeichnis/Administrativ/04-kreise.xlsx?__blob=publicationFile"
)
SCHEMA = "censusdata"
TABLE = "german_counties_info"

COUNTIES_ARGS = [URL, SCHEMA, TABLE]


@curry
def etl_german_counties_more_info(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download(url),
        transform,
        connect_to_db_and_insert_pandas_dataframe(schema, table),
    )(kwargs)
