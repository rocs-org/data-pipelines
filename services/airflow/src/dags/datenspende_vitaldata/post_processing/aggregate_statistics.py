import pandas as pd
import ramda as R
from psycopg2.sql import SQL, Literal

from postgres_helpers import create_db_context, teardown_db_context, DBContext
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    upsert_pandas_dataframe_to_table_in_schema_with_db_context,
)

DB_PARAMETERS = [
    "datenspende_derivatives",
    "daily_vital_statistics",
    ["one_value_per_user_and_type"],
]


def pipeline_for_aggregate_statistics_of_per_user_vitals(**kwargs) -> None:
    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    db_context = create_db_context()
    R.map(
        R.pipe(
            calculate_aggregated_statistics_of_user_vitals,
            upsert_pandas_dataframe_to_table_in_schema_with_db_context(
                db_context, *DB_PARAMETERS
            ),
        ),
        load_per_user_data(db_context),
    )
    teardown_db_context(db_context)


def calculate_aggregated_statistics_of_user_vitals(
    user_vital_data: pd.DataFrame,
) -> pd.DataFrame:
    return (
        user_vital_data.groupby(["user_id", "type", "source"])
        .agg({"value": ["mean", "std"]})
        .droplevel(level=0, axis="columns")
        .reset_index()
    )


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
            "SELECT user_id, type, source, value FROM datenspende.vitaldata WHERE user_id={user_id}"
        ).format(user_id=Literal(user_id)),
        db_context,
    )
