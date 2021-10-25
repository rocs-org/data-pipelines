import pandas
import ramda as R
from dags.database import DBContext, create_db_context
from dags.helpers.test_helpers import run_task_with_url

from .post_processing import (
    fetch_answers_from_db,
    transform_answers,
    post_processing_test_and_symptoms_answers,
)


def test_post_processing_loads_correct_answers(db_context: DBContext):
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    answers = fetch_answers_from_db("")
    assert set(answers["questionnaire"]) == set([10])


def test_transform_answers_returns_elements_as_list():
    assert transform_answers(QUESTIONS).equals(QUESTIONS_TRANSFORMED)


def test_etl_writes_to_db_correctly(db_context: DBContext):
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    post_processing_test_and_symptoms_answers(
        "datenspende_derivatives", "test_and_symptoms_answers", blub="blub"
    )

    answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers;",
        R.pipe(create_db_context, R.prop("connection"))(""),
    )
    assert len(answers) == 604


QUESTIONS = pandas.DataFrame(
    columns=["id", "user_id", "questionnaire", "question", "created_at", "element"],
    data=[
        [1, 1, 10, 1, 1, 1],
        [2, 1, 10, 2, 2, 1],
        [2, 1, 10, 2, 2, 2],
        [3, 1, 10, 3, 3, 1],
    ],
)
QUESTIONS_TRANSFORMED = pandas.DataFrame(
    columns=[
        "id",
        "answers",
        "user_id",
        "question_id",
        "created_at",
    ],
    data=[[1, "[1]", 1, 1, 1], [2, "[1, 2]", 1, 2, 2], [3, "[1]", 1, 3, 3]],
).set_index("id")
