import pandas as pd
import ramda as R

from src.dags.datenspende.case_detection_features import (
    extract_features_task,
    ONE_OFF_FEATURE_EXTRACTION_ARGS,
    WEEKLY_FEATURE_EXTRACTION_ARGS,
)
from src.lib.test_helpers import run_task_with_url
from postgres_helpers import DBContext


def test_feature_task_on_one_off_survey_results(pg_context: DBContext):

    # fill database
    # TODO: Add data for question ID 137 (new symptoms question)
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    # run feature extraction task
    extract_features_task(*ONE_OFF_FEATURE_EXTRACTION_ARGS)

    # load freature values and descriptions from postgres_helpers
    connection = R.prop("connection", pg_context)
    features_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.homogenized_features
        ;
        """,
        connection,
    ).dropna(axis=1, how="all")

    feature_names_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.homogenized_features_description
        ;
        """,
        connection,
    )

    connection.close()

    # same number of features that we put on
    assert len(feature_names_from_db) == 20
    assert len(features_from_db) == 49

    # features from db have expected format
    assert list(feature_names_from_db.sort_values("id").iloc[0].values) == [
        "War mindestens ein Testergebnis positiv, also wurde das Coronavirus bei Ihnen festgestellt?",
        "f10",
        True,
    ]

    # assert value of f91 is between 547 and 550
    assert 547 <= features_from_db.iloc[0]["f91"] <= 550


def test_feature_task_on_weeekly_survey_results(pg_context: DBContext):

    # fill database
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    # run feature extraction task
    extract_features_task(*WEEKLY_FEATURE_EXTRACTION_ARGS)

    # load freature values and descriptions from postgres_helpers
    connection = R.prop("connection", pg_context)

    print(
        pd.read_sql(
            """
            SELECT user_id, question, element FROM datenspende.answers WHERE user_id = '224410' and questionnaire = 2;
            """,
            connection,
        )
    )
    features_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.homogenized_features
        ;
        """,
        connection,
    )

    feature_names_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.homogenized_features_description
        ;
        """,
        connection,
    )

    connection.close()

    # same number of features that we put on
    assert len(feature_names_from_db) == 12
    assert len(features_from_db) == 7

    # feature names from db have expected format
    assert list(feature_names_from_db.sort_values("id").iloc[0].values) == [
        "Wie war das letzte Testergebnis?",
        "f10",
        True,
    ]

    # assert value of f91 is between 547 and 550
    assert 547 <= features_from_db.iloc[0]["f91"] <= 550
