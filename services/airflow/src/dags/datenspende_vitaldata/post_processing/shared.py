import pandas as pd
import ramda as R
from psycopg2.sql import SQL, Literal

from postgres_helpers import DBContext
from src.lib.dag_helpers import execute_query_and_return_dataframe


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
