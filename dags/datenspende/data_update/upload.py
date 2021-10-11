import polars
import ramda as R
from typing import Dict
from dags.helpers.dag_helpers import connect_to_db_and_insert_polars_dataframe


@R.curry
def upload(schema: str, data: Dict[str, polars.DataFrame]):
    for table, dataframe in data.items():
        print(f"uploading {table}...")
        connect_to_db_and_insert_polars_dataframe(schema, table, dataframe)
