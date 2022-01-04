import ramda as R
from dags.helpers.dag_helpers import connect_to_db_and_insert_polars_dataframe
from dags.datenspende.data_update.download import PolarsDataList


@R.curry
def upload(schema: str, data: PolarsDataList):
    R.map(upload_item(schema), data)


@R.curry
def upload_item(schema, item):
    (table, df) = item
    connect_to_db_and_insert_polars_dataframe(schema, table, df)
