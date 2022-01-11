import pandas as pd
import ramda as R
from typing import List
from datetime import datetime, timedelta
from src.dags.datenspende.case_detection_features.parameters import (
    WEEKLY_QUESTIONNAIRE,
    FEATURE_MAPPING,
)
from database import create_db_context


@R.curry
def restructure_features(
    questionnaire: int, questions, data: pd.DataFrame
) -> pd.DataFrame:
    # Unpack multiple choice question (symptoms)
    symptoms_answers = unstack_multiple_choice_question(questions["symptoms"], data)

    # select and restructure test results
    test_results_answers = select_answers_by_question_ids(
        [questions["test_result"], questions["vaccination_status"]], data
    )

    # select and restructure info about bodies
    body_info_answers = select_first_answer_by_question_ids(
        [questions[key] for key in ["sex", "height", "weight", "fitness", "age"]], data
    )

    # join
    features = symptoms_answers.join(test_results_answers, how="outer")

    features = pd.merge(
        features.reset_index(),
        body_info_answers.reset_index(),
        how="left",
        on=["user_id"],
    )

    return R.pipe(
        combine_columns(FEATURE_MAPPING),
        map_column_names_to_string_that_works_as_sql_identifier,
        add_test_dates_to_features(questionnaire),
        map_test_results_and_vaccination_status,
        lambda df: df.drop_duplicates(
            subset=["user_id", "test_week_start"], keep="last"
        ),
    )(features)


@R.curry
def unstack_multiple_choice_question(
    question_id: int, data: pd.DataFrame
) -> pd.DataFrame:
    # Unpack multiple choice question (symptoms)
    symptoms = data[["user_id", "questionnaire_session", "answer_id"]][
        data["question_id"] == question_id
    ]
    symptoms["yes"] = True

    symptoms = (
        symptoms.set_index(["user_id", "questionnaire_session", "answer_id"])
        .unstack("answer_id")
        .fillna(value=False)
    )
    symptoms.columns = symptoms.columns.droplevel(0)
    return symptoms


@R.curry
def select_answers_by_question_ids(
    question_ids: List[int], data: pd.DataFrame
) -> pd.DataFrame:
    # select and restructure test results
    result = (
        data[["user_id", "questionnaire_session", "answer_id", "question_id"]][
            data["question_id"].isin(question_ids)
        ]
        .set_index(["question_id", "user_id", "questionnaire_session"])
        .unstack(["question_id"])
    )
    result.columns = result.columns.droplevel(0)
    return result


@R.curry
def select_first_answer_by_question_ids(
    question_ids: List[int], data: pd.DataFrame
) -> pd.DataFrame:
    # select and restructure info about bodies
    result = (
        data[["user_id", "questionnaire_session", "answer_id", "question_id"]][
            data["question_id"].isin(question_ids)
        ]
        .groupby(["question_id", "user_id"])
        .first()
    ).drop(columns=["questionnaire_session"])
    result = result.unstack(["question_id"])
    result.columns = result.columns.droplevel(0)
    return result


def map_column_names_to_string_that_works_as_sql_identifier(
    features: pd.DataFrame,
) -> pd.DataFrame:
    df = features.copy()
    df.columns = [
        "f" + str(feature_id)
        if feature_id not in ["user_id", "questionnaire_session"]
        else feature_id
        for feature_id in df.columns
    ]
    return df


def map_test_results_and_vaccination_status(features: pd.DataFrame) -> pd.DataFrame:
    test_result_mapping = {
        52: True,
        53: False,
        782: True,
        783: False,
        784: None,
    }
    vaccination_status_mapping = {
        124: 728,
        125: 730,
    }
    return (
        features.replace({"f10": test_result_mapping})
        .replace({"f121": vaccination_status_mapping})
        .replace({pd.NA: None, float("nan"): None})
    )


@R.curry
def add_test_dates_to_features(
    questionnaire_id: int, features: pd.DataFrame
) -> pd.DataFrame:
    return R.pipe(
        R.if_else(R.equals(WEEKLY_QUESTIONNAIRE), get_weekly_dates, get_one_off_dates),
        lambda dates: features.join(
            dates, how="right", on=["user_id", "questionnaire_session"]
        ),
    )(questionnaire_id)


@R.curry
def collect_feature_names(questions, data: pd.DataFrame) -> pd.DataFrame:
    symptoms = data[["answer", "answer_id"]][
        data["question_id"] == questions["symptoms"]
    ].drop_duplicates("answer_id")
    symptoms.columns = ["description", "id"]
    symptoms["is_choice"] = False

    other_features = data[["question", "question_id"]][
        ~(data["question_id"] == questions["symptoms"])
    ].drop_duplicates("question_id")
    other_features.columns = ["description", "id"]
    other_features["is_choice"] = True

    feature_ids = symptoms.append(other_features)

    feature_ids["id"] = feature_ids["id"].apply(
        lambda feature_id: feature_id
        if feature_id not in FEATURE_MAPPING.keys()
        else FEATURE_MAPPING[feature_id]
    )

    feature_ids["id"] = feature_ids["id"].apply(
        lambda feature_id: "f" + str(feature_id)
        if not feature_id == "user_id"
        else feature_id
    )

    return feature_ids.append(
        pd.DataFrame(
            columns=["id", "description", "is_choice"],
            data=[
                ["user_id", "User Id", False],
                [
                    "test_week_start",
                    "First day of the week in which the test was taken",
                    False,
                ],
            ],
        )
    ).drop_duplicates("id")


@R.curry
def combine_columns(mapping: dict, dataframe: pd.DataFrame) -> pd.DataFrame:
    column_map = collect_keys_with_same_values_from(mapping)
    for combined_column, columns in column_map.items():
        try:
            dataframe[combined_column] = dataframe[columns].apply(
                R.reduce(xor, None), axis=1
            )
            dataframe.drop(columns=columns, inplace=True)
        except KeyError:
            pass
    return dataframe


def xor(a, b):
    if pd.isna(a) and pd.isna(b):
        return None
    elif pd.isna(a):
        return b
    elif pd.isna(b):
        return a
    else:
        return a or b


def collect_keys_with_same_values_from(dictionary: dict) -> dict:
    def add_value_to_list_if_key_in_dict(res, item):
        key, value = item
        if value in res:
            res[value] = res[value] + [key]
        else:
            res[value] = [key]
        return res

    return R.reduce(add_value_to_list_if_key_in_dict, {}, dictionary.items())


def get_symptom_ids_from_weekly() -> list:
    return R.pipe(
        create_db_context,
        R.prop("connection"),
        lambda connection: pd.read_sql(
            """
                SELECT
                    element
                FROM
                    datenspende.choice
                WHERE
                    question = 8
                ;
                """,
            connection,
        ),
        R.prop("element"),
        lambda series: series.values,
        list,
    )("")


def get_one_off_dates(*_) -> pd.DataFrame:
    connection = R.pipe(create_db_context, R.prop("connection"))("")
    test_dates = pd.read_sql(
        """
                SELECT
                    answers.user_id, answers.questionnaire_session, choice.text as date_text
                FROM
                    datenspende.answers, datenspende.choice
                WHERE
                    answers.questionnaire = 10 AND
                    answers.question = 83 AND
                    choice.question = answers.question AND
                    answers.element = choice.element
                ORDER BY
                    answers.user_id
                ;
                """,
        connection,
    )
    connection.close()
    test_dates["test_week_start"] = test_dates[["date_text"]].apply(
        R.pipe(
            lambda series: series.values,
            list,
            R.head,
            lambda date_text: datetime.strptime(date_text[:10], "%d.%m.%Y").date(),
        ),
        axis=1,
    )
    return test_dates.drop(columns=["date_text"]).set_index(
        ["user_id", "questionnaire_session"]
    )


def get_weekly_dates(*_) -> pd.DataFrame:
    connection = R.pipe(create_db_context, R.prop("connection"))("")
    test_dates = pd.read_sql(
        """
                SELECT
                    answers.user_id, answers.questionnaire_session, answers.created_at as date_raw
                FROM
                    datenspende.answers
                WHERE
                    answers.questionnaire = 2 AND
                    answers.question = 90
                ORDER BY
                    answers.user_id
                ;
                """,
        connection,
    )
    test_dates["test_week_start"] = test_dates[["date_raw"]].apply(
        lambda date_text: (
            datetime.fromtimestamp(date_text.values[0] / 1000) - timedelta(days=7)
        )
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .date(),
        axis=1,
    )
    connection.close()
    return test_dates.drop(columns=["date_raw"]).set_index(
        ["user_id", "questionnaire_session"]
    )
