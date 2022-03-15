import pandas as pd
import ramda as R
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    connect_to_db_and_upsert_pandas_dataframe_on_constraint,
)
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from postgres_helpers import create_db_context, teardown_db_context


def link_feature_records(**kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        link_subsequent_dates_for_same_user_id,
        write_links_to_db,
    )(kwargs)


def link_subsequent_dates_for_same_user_id(*_):
    db_context = create_db_context()
    questionnaire_sessions = execute_query_and_return_dataframe(
        """SELECT user_id, test_week_start FROM datenspende_derivatives.homogenized_features;""",
        db_context,
    )
    questionnaire_sessions["id"] = questionnaire_sessions.index

    questionnaire_sessions["next"] = (
        questionnaire_sessions.sort_values("test_week_start", ascending=True)
        .groupby("user_id", sort=False)["id"]
        .shift(-1)
        .replace({float("nan"): -1})
        .astype(int)
        .replace({-1: None})
    )

    questionnaire_sessions["previous"] = (
        questionnaire_sessions.sort_values("test_week_start", ascending=True)
        .groupby("user_id", sort=False)["id"]
        .shift(1)
        .replace({float("nan"): -1})
        .astype(int)
        .replace({-1: None})
    )

    teardown_db_context(db_context)
    return questionnaire_sessions.sort_values(
        ["user_id", "test_week_start"], ascending=True
    )


def write_links_to_db(records: pd.DataFrame) -> None:
    connect_to_db_and_upsert_pandas_dataframe_on_constraint(
        "datenspende_derivatives", "homogenized_features", ["unique_answers"], records
    )
