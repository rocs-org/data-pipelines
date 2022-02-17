import ramda as R

from clickhouse_helpers import (
    insert_dataframe,
    create_db_context,
    teardown_db_context,
    execute_sql,
)
from src.dags.datenspende.data_update.download import DataList


@R.curry
def load(table: str, data: DataList):
    context = create_db_context()
    R.map(R.pipe(R.nth(1), insert_dataframe(context, table)), data)

    # clickhouse does not support upsert, therefore, we have to use ReplicatedMergeTree table engine and OPTIMIZE to
    # clear out duplicates.
    execute_sql(context, f"OPTIMIZE TABLE {table}")
    teardown_db_context(context)
