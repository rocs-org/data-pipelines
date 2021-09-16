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
    teardown_db_context,
)


@curry
def connect_to_db_and_insert_pandas_dataframe(
    schema: str, table: str, data: pd.DataFrame
):
    return _connect_to_db_and_insert(_get_tuples_from_pd_dataframe, schema, table, data)


@curry
def connect_to_db_and_insert_polars_dataframe(
    schema: str, table: str, data: pd.DataFrame
):
    return _connect_to_db_and_insert(
        _get_tuples_from_polars_dataframe, schema, table, data
    )


@curry
def _connect_to_db_and_insert(
    tuple_getter, schema: str, table: str
) -> Callable[[DataFrame], DBContext]:
    return R.pipe(
        R.converge(
            _insert_pd_dataframe_to_postgres(tuple_getter),
            [
                lambda x: create_db_context(),
                R.always(schema),
                R.always(table),
                R.identity,
            ],
        ),
        teardown_db_context,
    )


@R.curry
def _insert_pd_dataframe_to_postgres(
    tuple_getter, context: DBContext, schema: str, table: str
):
    return R.converge(
        execute_values(context), [_build_insert_query(schema, table), tuple_getter]
    )


@R.curry
def _build_insert_query(schema: str, table: str) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        lambda columns: sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT DO NOTHING;"
        ).format(sql.Identifier(schema), sql.Identifier(table), columns),
    )


_get_columns = lambda df: sql.SQL(",").join(
    sql.Identifier(camel_case_to_snake_case(name)) for name in df.columns
)


_get_tuples_from_pd_dataframe = lambda df: [tuple(x) for x in df.to_numpy()]


_get_tuples_from_polars_dataframe = lambda df: df.rows()
