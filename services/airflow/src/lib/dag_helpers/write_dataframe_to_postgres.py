from typing import Callable, List
from postgres_helpers.execute_sql import execute_sql, DBContext

import polars
import pandas as pd
import ramda as R
from pandas import DataFrame
from psycopg2 import sql
from returns.pipeline import pipe


from postgres_helpers import (
    execute_values,
    camel_case_to_snake_case,
    create_db_context,
    teardown_db_context,
)


@R.curry
def connect_to_db_and_insert_pandas_dataframe(
    schema: str, table: str, data: pd.DataFrame
):
    return _connect_to_db_and_execute(
        _build_insert_query(schema, table), _get_tuples_from_pd_dataframe, data
    )


@R.curry
def connect_to_db_and_truncate_insert_pandas_dataframe(
    schema: str, table: str, data: pd.DataFrame
):
    _connect_to_db_and_execute_statement(
        sql.SQL("TRUNCATE TABLE {schema}.{table};").format(
            schema=sql.Identifier(schema), table=sql.Identifier(table)
        )
    )
    return _connect_to_db_and_execute(
        _build_insert_query(schema, table), _get_tuples_from_pd_dataframe, data
    )


@R.curry
def connect_to_db_and_insert_polars_dataframe(
    schema: str, table: str, data: polars.DataFrame
):
    return _connect_to_db_and_execute(
        _build_insert_query(schema, table), _get_tuples_from_polars_dataframe, data
    )


@R.curry
def connect_to_db_and_upsert_pandas_dataframe(
    schema: str, table: str, constraint_columns: List[str], data: pd.DataFrame
):
    return _connect_to_db_and_execute(
        _build_upsert_query(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {};",
            schema,
            table,
            constraint_columns,
        ),
        _get_tuples_from_pd_dataframe,
        data,
    )


@R.curry
def connect_to_db_and_upsert_pandas_dataframe_on_constraint(
    schema: str, table: str, constraints: List[str], data: pd.DataFrame
):
    return _connect_to_db_and_execute(
        build_upser_query_with_constraints(schema, table, constraints),
        _get_tuples_from_pd_dataframe,
        data,
    )


@R.curry
def upsert_pandas_dataframe_to_table_in_schema_with_db_context(
    db_context: DBContext,
    schema: str,
    table: str,
    constraints: List[str],
    data: pd.DataFrame,
) -> None:
    R.converge(
        execute_values,
        [
            R.always(db_context),
            build_upser_query_with_constraints(schema, table, constraints),
            _get_tuples_from_pd_dataframe,
        ],
    )(data)


def build_upser_query_with_constraints(schema: str, table: str, constraints: List[str]):
    return _build_upsert_query(
        "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ON CONSTRAINT {} DO UPDATE SET {};",
        schema,
        table,
        constraints,
    )


@R.curry
def connect_to_db_and_upsert_polars_dataframe(
    schema: str, table: str, constraint: list, data: polars.DataFrame
):
    return _connect_to_db_and_execute(
        _build_upsert_query(
            "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {};",
            schema,
            table,
            constraint,
        ),
        _get_tuples_from_polars_dataframe,
        data,
    )


@R.curry
def _connect_to_db_and_execute(query_builder, tuple_getter, data: pd.DataFrame):
    return R.pipe(
        R.converge(
            execute_values,
            [lambda x: create_db_context(), query_builder, tuple_getter],
        ),
        teardown_db_context,
    )(data)


_connect_to_db_and_execute_statement = R.pipe(
    R.converge(
        execute_sql,
        [lambda x: create_db_context(), R.identity],
    ),
    teardown_db_context,
)


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
    query: str, schema: str, table: str, constraint: list
) -> Callable[[DataFrame], sql.SQL]:
    return pipe(
        _get_columns,
        R.converge(
            sql.SQL(query).format,
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
