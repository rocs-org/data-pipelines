import pandas
import ramda as R
from database import DBContext
from src.lib.test_helpers import run_task_with_url

from .post_processing import (
    fetch_answers_from_db,
    collect_rows_with_same_id_but_different_element,
    separete_sessions_with_duplicate_user_and_question,
    post_processing_test_and_symptoms_answers,
)


def test_post_processing_loads_correct_answers(db_context: DBContext):
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    answers = fetch_answers_from_db("")
    assert set(answers["questionnaire"]) == {10}


def test_transform_answers_returns_elements_as_list():
    transformed = collect_rows_with_same_id_but_different_element(QUESTIONS)
    print(transformed.head())
    print(TRANSFORMED_QUESTIONS_WITH_DUPLICATES.head())
    assert transformed.equals(TRANSFORMED_QUESTIONS_WITH_DUPLICATES)


def test_etl_writes_to_db_correctly(db_context: DBContext):
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    post_processing_test_and_symptoms_answers(blub="blub")

    single_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers;",
        R.prop("connection", db_context),
    )
    assert len(single_answers) == 575

    duplicated_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers_duplicates;",
        R.prop("connection", db_context),
    )
    assert len(duplicated_answers) == 29


def test_split_answers_separates_single_from_duplicate_answers():
    answers = separete_sessions_with_duplicate_user_and_question(
        TRANSFORMED_QUESTIONS_WITH_DUPLICATES
    )
    print(answers["singles"].head())
    print(QUESTIONS_TRANSFORMED_SINGLE.head())
    assert answers["singles"].equals(QUESTIONS_TRANSFORMED_SINGLE)
    assert answers["duplicates"].equals(QUESTIONS_TRANSFORMED_DUPLICATES)


QUESTIONS = pandas.DataFrame(
    columns=[
        "id",
        "user_id",
        "questionnaire",
        "questionnaire_session",
        "question",
        "created_at",
        "element",
    ],
    data=[
        [1, 1, 10, 1, 1, 1, 1],  # questionnaire session with one question
        [2, 2, 10, 2, 1, 2, 1],  # two questionnaire sessions
        [3, 2, 10, 3, 1, 3, 2],  # with duplicate question
        [4, 2, 10, 4, 2, 4, 2],  # questionnaire session with multiple choice question
        [4, 2, 10, 4, 2, 4, 1],
    ],
)
TRANSFORMED_QUESTIONS_WITH_DUPLICATES = pandas.DataFrame(
    columns=[
        "id",
        "user_id",
        "session_id",
        "question_id",
        "created_at",
        "answers",
    ],
    data=[
        [1, 1, 1, 1, 1, "[1]"],
        [2, 2, 2, 1, 2, "[1]"],
        [3, 2, 3, 1, 3, "[2]"],
        [4, 2, 4, 2, 4, "[2, 1]"],
    ],
).set_index("id")
QUESTIONS_TRANSFORMED_SINGLE = pandas.DataFrame(
    columns=[
        "id",
        "user_id",
        "session_id",
        "question_id",
        "created_at",
        "answers",
    ],
    data=[
        [1, 1, 1, 1, 1, "[1]"],
        [4, 2, 4, 2, 4, "[2, 1]"],
    ],
).set_index("id")
QUESTIONS_TRANSFORMED_DUPLICATES = pandas.DataFrame(
    columns=[
        "id",
        "user_id",
        "session_id",
        "question_id",
        "created_at",
        "answers",
    ],
    data=[
        [2, 2, 2, 1, 2, "[1]"],
        [3, 2, 3, 1, 3, "[2]"],
    ],
).set_index("id")
