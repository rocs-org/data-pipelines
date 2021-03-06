import ramda as R

from postgres_helpers import create_db_context
from src.lib.dag_helpers import execute_query_and_return_dataframe


@R.curry
def load_test_and_symptoms_data(questionnaire: int, questions: dict):
    # select data above from users that have answered the tests and symptoms questionnaire (questionnaire = 10)
    return R.pipe(
        lambda *_: create_db_context(),
        execute_query_and_return_dataframe(
            """
                SELECT
                    answers.user_id, answers.questionnaire_session, answers.question AS question_id,
                    answers.element as answer_id, questions.text as question, choice.text as answer
                FROM
                    datenspende.answers, datenspende.choice, datenspende.questions
                WHERE
                    answers.questionnaire != 5 AND
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
                    user_id, questionnaire_session, question_id
                ;
            """.format(
                ",".join(
                    [str(question_id) for question_id in R.flatten(questions.values())]
                ),
                str(questionnaire),
            )
        ),
    )
