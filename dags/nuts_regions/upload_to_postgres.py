import pandas as pd
from dags.helpers.dag_helpers import write_dataframe_to_postgres
from dags.database import create_db_context
from returns.curry import curry
import ramda as R


@curry
def upload_to_postgres(schema: str, table: str, regions_data: pd.DataFrame):
    return R.converge(
        write_dataframe_to_postgres,
        [lambda x: create_db_context(), R.always(schema), R.always(table), R.identity],
    )(regions_data)
