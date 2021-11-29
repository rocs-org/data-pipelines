import ramda as R
from returns.curry import curry
from dags.update_hospitalizations.download_hospitalizations import (
    download_hospitalizations,
)
from dags.update_hospitalizations.transform_dataframe import (
    transform_dataframe,
)
from dags.helpers.dag_helpers.write_dataframe_to_postgres import (
    connect_to_db_and_insert_pandas_dataframe,
)
from dags.helpers.test_helpers import (
    set_env_variable_from_dag_config_if_present,
)

URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
SCHEMA = "coronacases"
TABLE = "german_hospitalizations"

HOSPITALIZATIONS_ARGS = [URL, SCHEMA, TABLE]

@curry
def etl_hospitalizations(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_hospitalizations(url),
        transform_dataframe,
        connect_to_db_and_insert_pandas_dataframe(schema, table),
    )(kwargs)