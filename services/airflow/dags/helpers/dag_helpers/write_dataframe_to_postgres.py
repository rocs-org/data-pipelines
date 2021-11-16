from typing import Callable

import polars
import pandas as pd
import ramda as R
from pandas import DataFrame
from psycopg2 import sql
from returns.curry import curry
from returns.pipeline import pipe


from database import (
    execute_values,
    camel_case_to_snake_case,
    create_db_context,
    teardown_db_context,
)


@curry
def connect_to_db_and_insert_pandas_dataframe(
    schema: str, table: str, data: pd.DataFrame
):
    return _connect_to_db_and_execute(
        _build_insert_query(schema, table), _get_tuples_from_pd_dataframe, data
    )


@curry
def connect_to_db_and_insert_polars_dataframe(
    schema: str, table: str, data: polars.DataFrame
):
    return _connect_to_db_and_execute(
        _build_insert_query(schema, table), _get_tuples_from_polars_dataframe, data
    )


@curry
def connect_to_db_and_upsert_pandas_dataframe(
    schema: str, table: str, constraint: str, data: pd.DataFrame
):
    return _connect_to_db_and_execute(
        _build_upsert_query(schema, table, constraint),
        _get_tuples_from_pd_dataframe,
        data,
    )


@curry
def connect_to_db_and_upsert_polars_dataframe(
    schema: str, table: str, constraint: list, data: polars.DataFrame
):
    return _connect_to_db_and_execute(
        _build_upsert_query(schema, table, constraint),
        _get_tuples_from_polars_dataframe,
        data,
    )


@curry
def _connect_to_db_and_execute(query_builder, tuple_getter, data: pd.DataFrame):
    return R.pipe(
        R.converge(
            execute_values,
            [lambda x: create_db_context(), query_builder, tuple_getter],
        ),
        teardown_db_context,
    )(data)


@R.curry
def _build_insert_query(schema: str, table: str) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        R.converge(
            sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT DO NOTHING;").format,
            [
                R.always(sql.Identifier(schema)),
                R.always(sql.Identifier(table)),
                _get_column_identifier_list,
            ],
        ),
    )


@R.curry
def _build_upsert_query(
    schema: str, table: str, constraint: list
) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        R.converge(
            sql.SQL(
                "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {};"
            ).format,
            [
                R.always(sql.Identifier(schema)),
                R.always(sql.Identifier(table)),
                _get_column_identifier_list,
                R.always(_get_constraint_columns_list(constraint)),
                _upsert_column_action,
            ],
        ),
    )


_get_columns = lambda df: R.map(camel_case_to_snake_case, df.columns)


_upsert_column_action = lambda columns: sql.SQL(", ").join(
    R.map(
        lambda column: sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(column), sql.Identifier(column)
        ),
        columns,
    ),
)


_get_column_identifier_list = lambda columns: sql.SQL(",").join(
    sql.Identifier(name) for name in columns
)


def _get_constraint_columns_list(constraint: list):
    return sql.SQL(",").join(sql.Identifier(name) for name in constraint)


_get_tuples_from_pd_dataframe = lambda df: [tuple(x) for x in df.to_numpy()]


_get_tuples_from_polars_dataframe = lambda df: df.rows()
