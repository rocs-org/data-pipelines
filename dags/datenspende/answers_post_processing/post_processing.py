import ramda as R
import pandas
from dags.database import create_db_context
from dags.helpers.dag_helpers import connect_to_db_and_insert_pandas_dataframe
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present


fetch_answers_from_db = R.converge(
    pandas.read_sql_query,
    [
        R.always(
            """
                SELECT
                    id, user_id, questionnaire, question, created_at, element
                FROM
                    datenspende.answers
                WHERE
                    questionnaire = 10
                ;
            """
        ),
        R.pipe(create_db_context, R.prop("connection")),
    ],
)


def transform_answers(answers: pandas.DataFrame) -> pandas.DataFrame:
    return (
        answers.groupby("id")
        .agg(
            {
                "element": R.pipe(lambda x: list(x), str),
                "user_id": "max",
                "question": "max",
                "created_at": "max",
            }
        )
        .rename(columns={"element": "answers", "question": "question_id"})
    )


def post_processing_test_and_symptoms_answers(schema: str, table: str, **kwargs):
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        fetch_answers_from_db,
        transform_answers,
        connect_to_db_and_insert_pandas_dataframe(schema, table),
        R.prop("credentials"),
    )(kwargs)


POST_PROCESSING_ARGS = ["datenspende_derivatives", "test_and_symptoms_answers"]
