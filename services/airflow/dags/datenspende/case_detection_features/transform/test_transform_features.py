import pandas as pd
import ramda as R
from typing import List

from dags.helpers.test_helpers import run_task_with_url
from dags.datenspende.case_detection_features.transform.transform_features import (
    collect_feature_names,
    combine_columns,
    collect_keys_with_same_values_from,
    xor,
    get_symptom_ids_from_weekly,
    restructure_features,
)
from dags.datenspende.case_detection_features.parameters import (
    ONE_OFF_QUESTIONS,
    WEEKLY_QUESTIONS,
)
from dags.datenspende.case_detection_features.extract.extract_survey_data import (
    load_test_and_symptoms_data,
)


def test_collect_keys_with_same_values():
    collected = collect_keys_with_same_values_from(
        {1: 3, 2: 3, "a": "c", "b": "c", "d": 5, 3: "c"}
    )
    assert collected[3] == [1, 2]
    assert collected["c"] == ["a", "b", 3]
    assert collected[5] == ["d"]


def test_xor():
    assert xor(pd.NA, True) is True
    assert xor(True, False) is True
    assert xor(True, True) is True
    assert xor(pd.NA, False) is False
    assert xor(False, False) is False
    assert pd.isna(xor(pd.NA, pd.NA))


def test_combine_columns():
    df = pd.DataFrame(
        columns=[1, 2],
        data=[
            [True, True],
            [True, pd.NA],
            [True, False],
            [False, False],
            [False, pd.NA],
        ],
    )

    combined = combine_columns({1: 3, 2: 3}, df)
    print(combined)
    assert list(combined.columns.values) == [3]
    assert list(combined.iloc[0].values) == [True]
    assert list(combined.iloc[1].values) == [True]
    assert list(combined.iloc[2].values) == [True]
    assert list(combined.iloc[3].values) == [False]
    assert list(combined.iloc[4].values) == [False]


def test_get_symptom_ids_from_db():
    assert get_symptom_ids_from_weekly() == ONE_OFF_SYMPTOM_IDS


def test_collect_feature_names_weekly_survey():
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    feature_names = R.pipe(
        load_test_and_symptoms_data(2, WEEKLY_QUESTIONS),
        collect_feature_names(WEEKLY_QUESTIONS),
    )("")

    assert isinstance(feature_names, pd.DataFrame)
    feature_names.sort_values("id")
    assert list(feature_names.iloc[0].values) == [
        "Ich hatte keine dieser genannten Symptome",
        "f49",
        False,
    ]
    print(set(list(feature_names["id"])).difference(set(FEATURE_IDS)))
    assert set(list(feature_names["id"])).issubset(set(FEATURE_IDS))


def test_collect_feature_names_one_off_survey():
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    feature_names = R.pipe(
        load_test_and_symptoms_data(10, ONE_OFF_QUESTIONS),
        collect_feature_names(ONE_OFF_QUESTIONS),
    )("")

    assert isinstance(feature_names, pd.DataFrame)
    feature_names.sort_values("id")
    assert list(feature_names.iloc[0].values) == ["Geschmacksstörung", "f42", False]
    assert set(list(feature_names["id"])).issubset(set(FEATURE_IDS))


def difference_between_dataframes_without_duplicates(
    dfs: List[pd.DataFrame],
) -> pd.DataFrame:
    return pd.concat(dfs).drop_duplicates(keep=False)


def sort_df_by_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.reindex(sorted(df.columns), axis=1)


difference_between_dataframes_without_duplicates_ignoring_column_order = R.pipe(
    R.map(sort_df_by_columns), difference_between_dataframes_without_duplicates
)


FEATURE_IDS = [
    "user_id",
    "f10",
    "f40",
    "f41",
    "f42",
    "f44",
    "f45",
    "f46",
    "f47",
    "f48",
    "f49",
    "f74",
    "f75",
    "f76",
    "f83",
    "f127",
    "f133",
    "f451",
    "f467",
    "f468",
    "f469",
    "f474",
    "f478",
]

FEATURE_ORDER_ONE_OFF = [
    "user_id",
    "f451",
    "f467",
    "f478",
    "f74",
    "f75",
    "f76",
    "f83",
    "f127",
    "f133",
    "f40",
    "f44",
    "f47",
    "f45",
    "f48",
    "f41",
    "f49",
    "f42",
    "f10",
]

FIRST_FEATURE_VALUES_ONE_OFF = [
    1095100,
    True,
    False,
    False,
    350.0,
    364.0,
    379.0,
    777.0,
    774.0,
    817.0,
    False,
    True,
    True,
    True,
    True,
    True,
    False,
    True,
    True,
]

FEATURE_ORDER_WEEKLY = [
    "user_id",
    "f10",
    "f45",
    "f49",
    "f74",
    "f75",
    "f76",
    "f127",
    "f133",
]
FIRST_FEATURE_VALUES_WEEKLY = [
    224410,
    None,
    False,
    True,
    349.0,
    362.0,
    375.0,
    773.0,
    816.0,
]

ONE_OFF_SYMPTOM_IDS = [
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
]


def test_restructure_features():

    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    features = R.pipe(
        load_test_and_symptoms_data(10, ONE_OFF_QUESTIONS),
        restructure_features(ONE_OFF_QUESTIONS),
    )("")
    feature_ids = features.columns.values
    assert len(feature_ids) == len(set(feature_ids))  # feature ids are unique
    assert len(features) == 5
    assert set(list(feature_ids)).issubset(set(FEATURE_IDS))
    features.sort_values("user_id")
    assert list(features.iloc[0].values) == FIRST_FEATURE_VALUES_ONE_OFF
