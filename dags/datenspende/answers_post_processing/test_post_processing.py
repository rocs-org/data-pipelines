import pandas
import ramda as R
from dags.database import DBContext, create_db_context
from dags.helpers.test_helpers import run_task_with_url

from .post_processing import (
    fetch_answers_from_db,
    collect_rows_with_same_id_but_different_element,
    separete_rows_with_duplicate_user_and_question,
    post_processing_test_and_symptoms_answers,
)


def test_post_processing_loads_correct_answers(db_context: DBContext):
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    answers = fetch_answers_from_db("")
    assert set(answers["questionnaire"]) == {10}


def test_transform_answers_returns_elements_as_list():
    transformed = collect_rows_with_same_id_but_different_element(QUESTIONS)
    assert transformed.equals(QUESTIONS_TRANSFORMED_SINGLE)


def test_etl_writes_to_db_correctly(db_context: DBContext):
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    post_processing_test_and_symptoms_answers(blub="blub")

    single_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers;",
        R.pipe(create_db_context, R.prop("connection"))(""),
    )
    assert len(single_answers) == 579
    assert (
        len(
            single_answers[
                ~single_answers.duplicated(
                    subset=["user_id", "question_id"], keep=False
                )
            ]
        )
        == 579
    )

    duplicated_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers_duplicates;",
        R.pipe(create_db_context, R.prop("connection"))(""),
    )
    assert len(duplicated_answers) == 25
    assert (
        len(
            duplicated_answers[
                duplicated_answers.duplicated(
                    subset=["user_id", "question_id"], keep=False
                )
            ]
        )
        == 25
    )


def test_split_answers_separates_single_from_duplicate_answers():
    answers = separete_rows_with_duplicate_user_and_question(TRANSFORMED_QUESTIONS_WITH_DUPLICATES)
    print(answers["singles"].head())
    print(QUESTIONS_TRANSFORMED_SINGLE.head())
    assert answers["singles"].equals(QUESTIONS_TRANSFORMED_SINGLE)
    assert answers["duplicates"].equals(QUESTIONS_TRANSFORMED_DUPLICATES)


QUESTIONS = pandas.DataFrame(
    columns=["id", "user_id", "questionnaire", "question", "created_at", "element"],
    data=[
        [1, 2, 10, 1, 1, 1],
        [2, 1, 10, 1, 1, 1],
        [3, 1, 10, 2, 2, 1],
        [3, 1, 10, 2, 2, 2],
        [4, 1, 10, 3, 3, 1],
    ],
)
TRANSFORMED_QUESTIONS_WITH_DUPLICATES = pandas.DataFrame(
    columns=[
        "id",
        "answers",
        "user_id",
        "question_id",
        "created_at",
    ],
    data=[
        [1, "[1]", 2, 1, 1],
        [2, "[1]", 1, 1, 1],
        [3, "[1, 2]", 1, 2, 2],
        [4, "[1]", 1, 3, 3],
        [5, "[1, 2]", 1, 4, 5],
        [6, "[1, 3, 4]", 1, 4, 6],
        [7, "[2]", 1, 5, 7],
        [8, "[3]", 1, 5, 8],
    ],
).set_index("id")
QUESTIONS_TRANSFORMED_SINGLE = pandas.DataFrame(
    columns=[
        "id",
        "answers",
        "user_id",
        "question_id",
        "created_at",
    ],
    data=[
        [1, "[1]", 2, 1, 1],
        [2, "[1]", 1, 1, 1],
        [3, "[1, 2]", 1, 2, 2],
        [4, "[1]", 1, 3, 3],
    ],
).set_index("id")
QUESTIONS_TRANSFORMED_DUPLICATES = pandas.DataFrame(
    columns=[
        "id",
        "answers",
        "user_id",
        "question_id",
        "created_at",
    ],
    data=[
        [5, "[1, 2]", 1, 4, 5],
        [6, "[1, 3, 4]", 1, 4, 6],
        [7, "[2]", 1, 5, 7],
        [8, "[3]", 1, 5, 8],
    ],
).set_index("id")
