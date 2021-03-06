import ramda as R
import pandas
from typing import Dict, Union
from psycopg2.sql import SQL, Composed
from postgres_helpers import with_db_context, DBContext
from src.lib.dag_helpers import connect_to_db_and_insert_pandas_dataframe
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present


@R.curry
def load_from_db_and_return_as_df(
    pg_context: DBContext, query: Union[str, SQL, Composed]
) -> pandas.DataFrame:
    return pandas.read_sql_query(query, R.prop("connection", pg_context))


fetch_answers_from_db = lambda *_: with_db_context(
    load_from_db_and_return_as_df,
    """
        SELECT
            id, user_id, questionnaire, questionnaire_session, question, created_at, element
        FROM
            datenspende.answers
        WHERE
            questionnaire = 10
        ;
    """,
)


def collect_rows_with_same_id_but_different_element(
    answers: pandas.DataFrame,
) -> pandas.DataFrame:
    return (
        answers.groupby("id")
        .agg(
            {
                "user_id": "max",
                "questionnaire_session": "max",
                "question": "max",
                "created_at": "max",
                "element": R.pipe(lambda x: list(x), str),
            }
        )
        .rename(
            columns={
                "element": "answers",
                "question": "question_id",
                "questionnaire_session": "session_id",
            }
        )
    )


def separete_sessions_with_duplicate_user_and_question(
    answers: pandas.DataFrame,
) -> Dict[str, pandas.DataFrame]:

    dupes = answers[answers.duplicated(subset=["user_id", "question_id"], keep=False)]
    sessions_with_duplicates = list(dupes["session_id"])
    return {
        "singles": answers[~answers["session_id"].isin(sessions_with_duplicates)],
        "duplicates": answers[answers["session_id"].isin(sessions_with_duplicates)],
    }


def post_processing_test_and_symptoms_answers(**kwargs):
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        fetch_answers_from_db,
        collect_rows_with_same_id_but_different_element,
        separete_sessions_with_duplicate_user_and_question,
        R.evolve(
            {
                "singles": connect_to_db_and_insert_pandas_dataframe(
                    "datenspende_derivatives", "test_and_symptoms_answers"
                ),
                "duplicates": connect_to_db_and_insert_pandas_dataframe(
                    "datenspende_derivatives", "test_and_symptoms_answers_duplicates"
                ),
            }
        ),
        R.prop("credentials"),
    )(kwargs)
