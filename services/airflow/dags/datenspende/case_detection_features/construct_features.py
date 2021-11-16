import pandas as pd
import ramda as R
from typing import List
from database import create_db_context, DBContext
from dags.helpers.dag_helpers import connect_to_db_and_insert_pandas_dataframe

WEEKLY_QUESTIONS = {
    "symptoms": 86,
    "date": 83,
    "age": 133,
    "sex": 127,
    "height": 74,
    "fittness": 76,
    "weight": 75,
}


def extract_test_and_symptoms_features_task(*_):

    return R.pipe(
        load_test_and_symptoms_data(10, WEEKLY_QUESTIONS),
        R.apply_spec(
            {
                "symptoms_features": restructure_features,
                "symptoms_features_description": collect_feature_names,
            }
        ),
        lambda d: list(d.items()),
        upload_all_dataframes,
    )("")


@R.curry
def upload_all_dataframes(dataframes: List[pd.DataFrame]):
    return R.map(
        lambda item: connect_to_db_and_insert_pandas_dataframe(
            "datenspende_derivatives", item[0], item[1]
        ),
        dataframes,
    )


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


def restructure_features(data: pd.DataFrame) -> pd.DataFrame:

    # select symptoms and pivot
    symptoms = data[["user_id", "answer_id"]][
        data["question_id"] == WEEKLY_QUESTIONS["symptoms"]
    ]
    symptoms["yes"] = True

    symptoms = (
        symptoms.set_index(["user_id", "answer_id"])
        .unstack("answer_id")
        .fillna(value=False)
    )
    symptoms.columns = symptoms.columns.droplevel(0)

    # restructure other answers
    other_answers = (
        data[["user_id", "answer_id", "question_id"]][
            ~(data["question_id"] == WEEKLY_QUESTIONS["symptoms"])
        ]
        .groupby(["question_id", "user_id"])
        .first()
    )
    other_answers = other_answers.unstack(["question_id"])
    other_answers.columns = other_answers.columns.droplevel(0)

    # join
    features = symptoms.join(other_answers).reset_index()

    # cast feature_ids to string
    features.columns = [
        "f" + str(feature_id) if not feature_id == "user_id" else feature_id
        for feature_id in features.columns
    ]
    return features


def collect_feature_names(data: pd.DataFrame) -> pd.DataFrame:
    symptoms = data[["answer", "answer_id"]][
        data["question_id"] == WEEKLY_QUESTIONS["symptoms"]
    ].drop_duplicates()
    symptoms.columns = ["description", "id"]
    symptoms["is_choice"] = False

    other_features = data[["question", "question_id"]][
        ~(data["question_id"] == WEEKLY_QUESTIONS["symptoms"])
    ].drop_duplicates()
    other_features.columns = ["description", "id"]
    other_features["is_choice"] = True

    feature_ids = symptoms.append(other_features)

    feature_ids["id"] = feature_ids["id"].apply(
        lambda feature_id: "f" + str(feature_id)
        if not feature_id == "user_id"
        else feature_id
    )

    return feature_ids.append(
        pd.DataFrame(
            columns=["id", "description", "is_choice"],
            data=[["user_id", "User Id", False]],
        )
    )


@R.curry
def execute_query_and_return_dataframe(query: str, context: DBContext):
    return pd.read_sql(query, con=context["connection"])
