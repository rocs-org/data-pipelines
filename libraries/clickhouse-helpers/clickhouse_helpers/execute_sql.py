import re
from typing import Any

import pandas as pd
import ramda as R
from pandahouse import to_clickhouse

from .types import DBContext


@R.curry
def execute_sql(context: DBContext, sql: str, data=None) -> Any:
    return context["connection"].execute(sql, data)


@R.curry
def query_dataframe(context: DBContext, sql: str) -> pd.DataFrame:
    return context["connection"].query_dataframe(sql)


@R.curry
def insert_dataframe(context: DBContext, table: str, data: pd.DataFrame):
    credentials = context["credentials"]
    to_clickhouse(
        data,
        table,
        connection={
            "host": f"http://{credentials['host']}:8123",
            "database": credentials["database"],
        },
    )


@R.curry
def _create_database(context: DBContext) -> DBContext:
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context


def camel_case_to_snake_case(string: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def snake_case_to_camel_case(string: str):
    return "".join(word.title() for word in string.split("_"))
