import ramda as R
from returns.curry import curry
from .download import download
from .transform import transform
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present
from dags.helpers.dag_helpers import connect_to_db_and_insert_pandas_dataframe


URL = (
    "https://gisco-services.ec.europa.eu/tercet/NUTS-2021/pc2020_DE_NUTS-2021_v3.0.zip"
)
SCHEMA = "censusdata"
TABLE = "german_zip_codes"

ZIP_ARGS = [URL, SCHEMA, TABLE]


@curry
def etl_german_zip_codes(url: str, schema: str, table: str, *_, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download(url),
        transform,
        connect_to_db_and_insert_pandas_dataframe(schema, table),
    )(kwargs)
