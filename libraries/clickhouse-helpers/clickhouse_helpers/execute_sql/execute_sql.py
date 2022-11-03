from typing import Any

import pandas as pd
import ramda as R

from clickhouse_helpers.types import DBContext


@R.curry
def execute_sql(context: DBContext, sql: str, data=None) -> Any:
    return context["connection"].execute(sql, data)


@R.curry
def query_dataframe(context: DBContext, sql: str) -> pd.DataFrame:
    return context["connection"].query_dataframe(sql)


@R.curry
def insert_dataframe(context: DBContext, table: str, data: pd.DataFrame):
    return context["connection"].insert_dataframe(
        f"INSERT INTO {table} VALUES", data, settings=dict(types_check=True)
    )


@R.curry
def _create_database(context: DBContext) -> DBContext:
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context
