from typing import Callable, List

import pandas as pd
import ramda as R
from pandas import DataFrame
from psycopg2 import sql
from returns.curry import curry
from returns.pipeline import pipe

from dags.database import (
    DBContext,
    execute_values,
    camel_case_to_snake_case,
    create_db_context,
)


@curry
def connect_to_db_and_insert(schema: str, table: str, data: pd.DataFrame):
    return R.converge(
        insert_dataframe_to_postgres,
        [lambda x: create_db_context(), R.always(schema), R.always(table), R.identity],
    )(data)


@R.curry
def insert_dataframe_to_postgres(
    context: DBContext, schema: str, table: str, data: DataFrame
):
    return R.converge(
        execute_values(context), [_build_insert_query(schema, table), _get_tuples]
    )(data)


@R.curry
def _build_insert_query(schema: str, table: str) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        lambda columns: sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT DO NOTHING;"
        ).format(sql.Identifier(schema), sql.Identifier(table), columns),
    )


def _get_columns(df: DataFrame) -> sql.SQL:
    return sql.SQL(",").join(
        sql.Identifier(camel_case_to_snake_case(name)) for name in df.columns
    )


def _get_tuples(df: DataFrame) -> List:
    return [tuple(x) for x in df.to_numpy()]
