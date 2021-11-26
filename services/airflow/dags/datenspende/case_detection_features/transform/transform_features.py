import pandas as pd
import ramda as R
from datetime import datetime, timedelta
from dags.datenspende.case_detection_features.parameters import (
    WEEKLY_QUESTIONNAIRE,
    FEATURE_MAPPING,
)
from database import create_db_context


@R.curry
def restructure_features(
    questionnaire: int, questions, data: pd.DataFrame
) -> pd.DataFrame:
    # Unpack multiple choice question (symptoms)
    symptoms = data[["user_id", "questionnaire_session", "answer_id"]][
        data["question_id"] == questions["symptoms"]
    ]
    symptoms["yes"] = True

    symptoms = (
        symptoms.set_index(["user_id", "questionnaire_session", "answer_id"])
        .unstack("answer_id")
        .fillna(value=False)
    )
    symptoms.columns = symptoms.columns.droplevel(0)

    # select and restructure test results
    test_results = (
        data[["user_id", "questionnaire_session", "answer_id", "question_id"]][
            data["question_id"] == questions["test_result"]
        ]
        .set_index(["question_id", "user_id", "questionnaire_session"])
        .unstack(["question_id"])
    )
    test_results.columns = test_results.columns.droplevel(0)

    # select and restructure info about bodies
    body_info = (
        data[["user_id", "questionnaire_session", "answer_id", "question_id"]][
            data["question_id"].isin(
                [questions[key] for key in ["sex", "height", "weight", "fitness"]]
            )
        ]
        .groupby(["question_id", "user_id", "questionnaire_session"])
        .first()
    )
    body_info = body_info.unstack(["question_id"])
    body_info.columns = body_info.columns.droplevel(0)

    # join
    features = symptoms.join(test_results, how="outer")

    features = (
        features.reset_index()
        .merge(
            body_info.reset_index().drop(columns=["questionnaire_session"]),
            how="left",
            on="user_id",
        )
        .replace(float("nan"), None)
    )
    # map features and combine boolean columns
    features = combine_columns(FEATURE_MAPPING, features)
    # cast feature_ids to string
    features.columns = [
        "f" + str(feature_id)
        if feature_id not in ["user_id", "questionnaire_session"]
        else feature_id
        for feature_id in features.columns
    ]

    # cast test results to boolean
    test_result_mapping = {
        52: True,
        53: False,
        782: True,
        783: False,
        784: None,
        float("nan"): None,
    }
    features = features.replace({"f10": test_result_mapping}).replace({pd.NA: None})
    return R.pipe(
        R.if_else(R.equals(WEEKLY_QUESTIONNAIRE), get_weekly_dates, get_one_off_dates),
        lambda df: df.set_index(["user_id", "questionnaire_session"]),
        lambda dates: features.join(
            dates, how="left", on=["user_id", "questionnaire_session"]
        ),
        lambda df: df.drop(columns=["questionnaire_session"]),
    )(questionnaire)


@R.curry
def collect_feature_names(questions, data: pd.DataFrame) -> pd.DataFrame:
    symptoms = data[["answer", "answer_id"]][
        data["question_id"] == questions["symptoms"]
    ].drop_duplicates()
    symptoms.columns = ["description", "id"]
    symptoms["is_choice"] = False

    other_features = data[["question", "question_id"]][
        ~(data["question_id"] == questions["symptoms"])
    ].drop_duplicates()
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
    )


def combine_columns(mapping: dict, dataframe: pd.DataFrame) -> pd.DataFrame:
    column_map = collect_keys_with_same_values_from(mapping)
    for combined_column, columns in column_map.items():
        try:
            dataframe[combined_column] = dataframe[columns].apply(
                R.reduce(xor, pd.NA), axis=1
            )
            dataframe.drop(columns=columns, inplace=True)
        except KeyError:
            pass
    return dataframe


def xor(a, b):
    if pd.isna(a) and pd.isna(b):
        return pd.NA
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


def get_symptom_ids_from_weekly():
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
    return test_dates.drop(columns=["date_text"])


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
    return test_dates.drop(columns=["date_raw"])
