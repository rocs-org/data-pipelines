import pandas as pd
import ramda as R
import pytest
from datetime import datetime
from database import DBContext
from typing import List

from dags.helpers.test_helpers import run_task_with_url
from dags.datenspende.case_detection_features.transform.transform_features import (
    collect_feature_names,
    combine_columns,
    collect_keys_with_same_values_from,
    xor,
    get_symptom_ids_from_weekly,
    restructure_features,
    get_one_off_dates,
    get_weekly_dates,
)
from dags.datenspende.case_detection_features.parameters import (
    ONE_OFF_QUESTIONS,
    WEEKLY_QUESTIONS,
    WEEKLY_QUESTIONNAIRE,
    ONE_OFF_QUESTIONNAIRE,
)
from dags.datenspende.case_detection_features.extract.extract_survey_data import (
    load_test_and_symptoms_data,
)


def test_collect_feature_names_weekly_survey(prepared_db: DBContext):

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


def test_collect_feature_names_one_off_survey(prepared_db: DBContext):

    feature_names = R.pipe(
        load_test_and_symptoms_data(10, ONE_OFF_QUESTIONS),
        collect_feature_names(ONE_OFF_QUESTIONS),
    )("")

    assert isinstance(feature_names, pd.DataFrame)
    feature_names.sort_values("id")
    assert list(feature_names.iloc[0].values) == ["Geschmacksstörung", "f42", False]
    assert set(list(feature_names["id"])).issubset(set(FEATURE_IDS))


def test_get_date_for_one_off_surveys(prepared_db):
    dates = get_one_off_dates("")
    print(dates)
    assert isinstance(dates, pd.DataFrame)
    assert dataframes_equal(
        dates.head(n=1),
        pd.DataFrame(
            columns=["user_id", "test_week_start", "questionnaire_session"],
            data=[[8382, datetime.strptime("2020-12-28", "%Y-%m-%d").date(), 1394]],
        ),
    )


def test_get_date_for_weekly_surveys(prepared_db):

    dates = get_weekly_dates("")
    assert isinstance(dates, pd.DataFrame)
    assert dataframes_equal(
        dates.head(n=1),
        pd.DataFrame(
            columns=["user_id", "questionnaire_session", "test_week_start"],
            data=[[224410, 1522, datetime.strptime("2021-10-06", "%Y-%m-%d").date()]],
        ),
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
    assert list(combined.columns.values) == [3]
    assert list(combined.iloc[0].values) == [True]
    assert list(combined.iloc[1].values) == [True]
    assert list(combined.iloc[2].values) == [True]
    assert list(combined.iloc[3].values) == [False]
    assert list(combined.iloc[4].values) == [False]


def test_get_symptom_ids_from_db(prepared_db):
    assert get_symptom_ids_from_weekly() == ONE_OFF_SYMPTOM_IDS


def test_restructure_one_off_features(prepared_db):

    features = R.pipe(
        load_test_and_symptoms_data(ONE_OFF_QUESTIONNAIRE, ONE_OFF_QUESTIONS),
        restructure_features(ONE_OFF_QUESTIONNAIRE, ONE_OFF_QUESTIONS),
    )("")
    feature_ids = features.columns.values
    assert len(feature_ids) == len(set(feature_ids))  # feature ids are unique
    assert len(features) == 47
    assert set(list(feature_ids)).issubset(set(FEATURE_IDS))
    assert list(features.iloc[0].values) == FIRST_FEATURE_VALUES_ONE_OFF


def test_restructure_weekly_features(prepared_db):

    features = R.pipe(
        load_test_and_symptoms_data(WEEKLY_QUESTIONNAIRE, WEEKLY_QUESTIONS),
        restructure_features(WEEKLY_QUESTIONNAIRE, WEEKLY_QUESTIONS),
    )("")
    feature_ids = features.columns.values
    assert len(feature_ids) == len(set(feature_ids))  # feature ids are unique
    assert len(features) == 7
    assert set(list(feature_ids)).issubset(set(FEATURE_IDS))
    assert list(features.iloc[0].values) == FIRST_FEATURE_VALUES_WEEKLY


@pytest.fixture
def prepared_db(db_context: DBContext):
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )
    return db_context


def difference_between_dataframes_without_duplicates(
    dfs: List[pd.DataFrame],
) -> pd.DataFrame:
    return pd.concat(dfs).drop_duplicates(keep=False)


def sort_df_by_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.reindex(sorted(df.columns), axis=1)


difference_between_dataframes_without_duplicates_ignoring_column_order = R.pipe(
    R.map(sort_df_by_columns), difference_between_dataframes_without_duplicates
)


def dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    diff = difference_between_dataframes_without_duplicates_ignoring_column_order(
        [df1, df2]
    )
    return len(list(diff.values)) == 0


FEATURE_IDS = [
    "user_id",
    "test_week_start",
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
    "f467",
    "f468",
    "f469",
    "f474",
    "f478",
]

FEATURE_ORDER_ONE_OFF = [
    "user_id",
    "f478",
    "f74",
    "f75",
    "f76",
    "f127",
    "f40",
    "f44",
    "f47",
    "f45",
    "f48",
    "f41",
    "f49",
    "f42",
    "f10",
    "test_week_start",
]


FIRST_FEATURE_VALUES_ONE_OFF = [
    8382,
    None,
    351.0,
    367.0,
    378.0,
    774.0,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    False,
    datetime.strptime("2020-12-28", "%Y-%m-%d").date(),
]


FEATURE_ORDER_WEEKLY = [
    "user_id",
    "f45",
    "f49",
    "f10",
    "f74",
    "f75",
    "f76",
    "f127",
    "test_week_start",
]

FIRST_FEATURE_VALUES_WEEKLY = [
    224410,
    False,
    True,
    None,
    349,
    362,
    375,
    773,
    datetime.strptime("2021-10-06", "%Y-%m-%d").date(),
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
