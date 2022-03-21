from typing import Callable

import pandas as pd
import ramda as R
from psycopg2.sql import SQL, Literal

from postgres_helpers import DBContext, create_db_context, teardown_db_context
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    upsert_pandas_dataframe_to_table_in_schema_with_db_context,
)
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present


def load_per_user_data(pg_context: DBContext) -> pd.DataFrame:
    for user_id in load_distinct_user_ids(pg_context):
        yield load_user_vitals(pg_context, user_id)


def load_distinct_user_ids(db_context: DBContext) -> pd.DataFrame:
    return R.pipe(
        execute_query_and_return_dataframe(
            SQL("SELECT DISTINCT user_id FROM datenspende.vitaldata;"),
        ),
        lambda df: df["user_id"].values,
        R.map(int),
    )(db_context)


def load_user_vitals(db_context: DBContext, user_id: int) -> pd.DataFrame:
    return execute_query_and_return_dataframe(
        SQL(
            """
            SELECT
                user_id, type, source, value, date
            FROM
                datenspende.vitaldata
            WHERE
                user_id={user_id}
            ORDER BY
                user_id
            """
        ).format(user_id=Literal(user_id)),
        db_context,
    )


@R.curry
def post_processing_vitals_pipeline_factory(
    aggregator: Callable[[pd.DataFrame], pd.DataFrame], db_parameters
):
    def pipeline(**kwargs) -> None:
        set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
        db_context = create_db_context()
        R.map(
            R.pipe(
                aggregator,
                upsert_pandas_dataframe_to_table_in_schema_with_db_context(
                    db_context, *db_parameters
                ),
            ),
            load_per_user_data(db_context),
        )
        teardown_db_context(db_context)

    return pipeline
