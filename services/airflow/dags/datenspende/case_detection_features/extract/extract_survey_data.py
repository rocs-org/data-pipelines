import pandas as pd
import ramda as R

from database import create_db_context, DBContext


@R.curry
def load_test_and_symptoms_data(questionnaire: int, questions: dict):
    # select data above from users that have answered the tests and symptoms questionnaire (questionnaire = 10)
    return R.pipe(
        lambda *_: create_db_context(),
        execute_query_and_return_dataframe(
            """
        SELECT
            answers.user_id, answers.question AS question_id, answers.element as answer_id, questions.text as question,
            choice.text as answer
        FROM
            datenspende.answers, datenspende.choice, datenspende.questions
        WHERE
            answers.question IN ({}) AND
            answers.element = choice.element AND
            answers.question = questions.id AND
            answers.user_id IN (
                SELECT
                    user_id
                FROM
                    datenspende.questionnaire_session
                WHERE
                    questionnaire = {} AND
                    completed_at IS NOT NULL
                )
        ORDER BY
            user_id
    ;
    """.format(
                ",".join([str(question_id) for question_id in questions.values()]),
                str(questionnaire),
            )
        ),
    )


@R.curry
def execute_query_and_return_dataframe(query: str, context: DBContext):
    return pd.read_sql(query, con=context["connection"])
