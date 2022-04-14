from psycopg2.sql import SQL, Identifier

from src.lib.dag_helpers import execute_query_and_return_dataframe
from .aggregate_statistics_before_infection import (
    DB_PARAMETERS,
    aggregate_statistics_before_infection,
)
from .pivot_tables_test import setup_vitaldata_in_db

schema, table = DB_PARAMETERS


def test_aggregate_statistics_creates_table_with_data(pg_context):
    setup_vitaldata_in_db("http://static-files/thryve/export.7z")
    setup_vitaldata_in_db("http://static-files/thryve/exportStudy.7z")

    aggregate_statistics_before_infection(*DB_PARAMETERS)

    res = execute_query_and_return_dataframe(
        SQL(
            """
            SELECT * FROM {schema}.{table}
        """
        ).format(schema=Identifier(schema), table=Identifier(table)),
        pg_context,
    )

    assert list(res.columns) == ["user_id", "type", "source", "mean", "std"]
