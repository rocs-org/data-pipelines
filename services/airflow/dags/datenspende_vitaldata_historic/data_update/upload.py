import ramda as R
from dags.helpers.dag_helpers import connect_to_db_and_upsert_polars_dataframe
from dags.datenspende.data_update.download import DataList


@R.curry
def upload(schema: str, data: DataList):
    R.map(upload_item(schema), data)


@R.curry
def upload_item(schema, item):
    (table, constraint, df) = item
    connect_to_db_and_upsert_polars_dataframe(schema, table, constraint, df)
