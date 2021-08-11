from typing import Callable, List

import ramda as R
from pandas import DataFrame
from psycopg2 import sql
from returns._internal.pipeline.pipe import pipe

from dags.database import DBContext, execute_values, camel_case_to_snake_case


@R.curry
def write_dataframe_to_postgres(
    context: DBContext, schema: str, table: str, data: DataFrame
):
    return R.converge(
        execute_values(context), [_build_query(schema, table), _get_tuples]
    )(data)


@R.curry
def _build_query(schema: str, table: str) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        lambda columns: sql.SQL("INSERT INTO {}.{} ({}) VALUES %s;").format(
            sql.Identifier(schema), sql.Identifier(table), columns
        ),
    )


def _get_columns(df: DataFrame) -> sql.SQL:
    return sql.SQL(",").join(
        sql.Identifier(camel_case_to_snake_case(name)) for name in df.columns
    )


def _get_tuples(df: DataFrame) -> List:
    return [tuple(x) for x in df.to_numpy()]
