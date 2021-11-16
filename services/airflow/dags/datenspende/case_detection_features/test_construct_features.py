import pandas as pd
import ramda as R

from database import DBContext, create_db_context

from dags.helpers.test_helpers import run_task_with_url
from dags.datenspende.case_detection_features.construct_features import (
    load_test_and_symptoms_data,
    restructure_features,
    collect_feature_names,
    extract_test_and_symptoms_features_task,
    WEEKLY_QUESTIONS,
)


def test_extract_features_from_tests_and_symptoms_questionnarie():

    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    loader = load_test_and_symptoms_data(10, WEEKLY_QUESTIONS)

    features = loader("")
    print(features.head())

    assert isinstance(features, pd.DataFrame)
    assert len(features.index) == 391
    assert list(features.iloc[0].values) == [
        8382,
        83,
        525,
        "Wann wurde der Test durchgeführt?",
        "28.12.2020 bis 03.01.2021",
    ]


def test_restructure_features():

    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    features = R.pipe(
        load_test_and_symptoms_data(10, WEEKLY_QUESTIONS), restructure_features
    )("")
    feature_ids = features.columns.values
    assert len(feature_ids) == len(set(feature_ids))  # feature ids are unique
    assert len(features) == 5
    assert list(feature_ids) == FEATURE_IDS
    assert list(features.iloc[0].values) == FIRST_FEATURE_VALUE


def test_collect_feature_names():
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    feature_names = R.pipe(
        load_test_and_symptoms_data(10, WEEKLY_QUESTIONS), collect_feature_names
    )("")

    assert isinstance(feature_names, pd.DataFrame)
    assert list(feature_names.iloc[0].values) == ["Fieber über 38°C", "f451", False]
    assert set(list(feature_names["id"])) == set(FEATURE_IDS)


def test_feature_task(db_context: DBContext):

    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    extract_test_and_symptoms_features_task()

    connection = R.pipe(create_db_context, R.prop("connection"))("")
    features_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.symptoms_features
        ;
        """,
        connection,
    )

    feature_names_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.symptoms_features_description
        ;
        """,
        connection,
    )

    connection.close()

    # same number of features that we put on
    assert len(feature_names_from_db) == 19
    assert len(features_from_db) == 5

    # all features in feature data are listed in feature description
    assert set(list(features_from_db.columns.values)) == set(
        list(feature_names_from_db["id"].values)
    )
    list(feature_names_from_db.iloc[0].values) == ["Fieber über 38°C", "f451", False]
    assert list(features_from_db.iloc[0].values) == FIRST_FEATURE_VALUE


FEATURE_IDS = [
    "user_id",
    "f451",
    "f452",
    "f453",
    "f467",
    "f470",
    "f471",
    "f472",
    "f473",
    "f476",
    "f477",
    "f478",
    "f479",
    "f74",
    "f75",
    "f76",
    "f83",
    "f127",
    "f133",
]

FIRST_FEATURE_VALUE = [
    1095100,
    True,
    False,
    True,
    False,
    True,
    True,
    True,
    True,
    True,
    True,
    False,
    False,
    350.0,
    364.0,
    379.0,
    777.0,
    774.0,
    817.0,
]
