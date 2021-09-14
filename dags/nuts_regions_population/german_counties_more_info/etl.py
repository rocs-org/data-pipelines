import ramda as R
from returns.curry import curry
from .download import download
from .transform import transform
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present
from dags.helpers.dag_helpers import connect_to_db_and_insert


URL = (
    "https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/"
    + "Gemeindeverzeichnis/Administrativ/04-kreise.xlsx;"
    + "jsessionid=54A8E9B7D4B7A2D755B8A5B1FA599C4F.live711?__blob=publicationFile"
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
        connect_to_db_and_insert(schema, table),
    )(kwargs)
