import pandas as pd
import ramda as R

from dags.datenspende.case_detection_features.parameters import FEATURE_MAPPING
from database import create_db_context


@R.curry
def restructure_features(questions, data: pd.DataFrame) -> pd.DataFrame:
    # select and restructure symptoms
    symptoms = data[["user_id", "answer_id"]][
        data["question_id"] == questions["symptoms"]
    ]
    symptoms["yes"] = True

    symptoms = (
        symptoms.set_index(["user_id", "answer_id"])
        .unstack("answer_id")
        .fillna(value=False)
    )
    symptoms.columns = symptoms.columns.droplevel(0)

    # select and restructure other answers
    other_answers = (
        data[["user_id", "answer_id", "question_id"]][
            ~(data["question_id"] == questions["symptoms"])
        ]
        .groupby(["question_id", "user_id"])
        .first()
    )
    other_answers = other_answers.unstack(["question_id"])
    other_answers.columns = other_answers.columns.droplevel(0)

    # join
    features = symptoms.join(other_answers).reset_index()

    # map features and combine boolean columns
    features = combine_columns(FEATURE_MAPPING, features)

    # cast feature_ids to string
    features.columns = [
        "f" + str(feature_id) if not feature_id == "user_id" else feature_id
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
    features.replace({"f10": test_result_mapping}, inplace=True)

    return features


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
            data=[["user_id", "User Id", False]],
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
            print(f"{combined_column} not in columns")
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
