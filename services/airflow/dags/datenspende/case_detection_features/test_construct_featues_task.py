import pandas as pd
import ramda as R

from dags.datenspende.case_detection_features import (
    extract_features_task,
    ONE_OFF_FEATURE_EXTRACTION_ARGS,
    WEEKLY_FEATURE_EXTRACTION_ARGS,
)
from dags.datenspende.case_detection_features.transform.test_transform_features import (
    FEATURE_ORDER_ONE_OFF,
    FIRST_FEATURE_VALUES_ONE_OFF,
    difference_between_dataframes_without_duplicates_ignoring_column_order,
    FEATURE_ORDER_WEEKLY,
    FIRST_FEATURE_VALUES_WEEKLY,
)
from dags.helpers.test_helpers import run_task_with_url
from database import DBContext, create_db_context


def test_feature_task_on_one_off_survey_results(db_context: DBContext):

    # fill database
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    # run feature extraction task
    extract_features_task(*ONE_OFF_FEATURE_EXTRACTION_ARGS)

    # load freature values and descriptions from database
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
    ).dropna(axis=1, how="all")

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
    assert set(list(features_from_db.columns.values)).issubset(
        set(list(feature_names_from_db["id"].values))
    )

    # features from db have expected format
    assert list(feature_names_from_db.iloc[0].values) == [
        "Geschmacksstörung",
        "f42",
        False,
    ]

    # feature values are as expected
    first_feature_values = pd.DataFrame(
        columns=FEATURE_ORDER_ONE_OFF, data=[FIRST_FEATURE_VALUES_ONE_OFF]
    )

    difference = difference_between_dataframes_without_duplicates_ignoring_column_order(
        [
            first_feature_values,
            features_from_db.head(n=1),
        ]
    )

    assert list(difference.values) == []


def test_feature_task_on_weeekly_survey_results(db_context: DBContext):

    # fill database
    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    # run feature extraction task
    extract_features_task(*WEEKLY_FEATURE_EXTRACTION_ARGS)

    # load freature values and descriptions from database
    connection = R.pipe(create_db_context, R.prop("connection"))("")
    features_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.weekly_features
        ;
        """,
        connection,
    ).dropna(axis=1, how="all")

    feature_names_from_db = pd.read_sql(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.weekly_features_description
        ;
        """,
        connection,
    )

    connection.close()

    # same number of features that we put on
    assert len(feature_names_from_db) == 9
    assert len(features_from_db) == 7

    # all features in feature data are listed in feature description
    assert set(list(features_from_db.columns.values)).issubset(
        set(list(feature_names_from_db["id"].values))
    )

    # features from db have expected format
    assert list(feature_names_from_db.iloc[0].values) == [
        "Ich hatte keine dieser genannten Symptome",
        "f49",
        False,
    ]

    # feature values are as expected
    first_feature_values = pd.DataFrame(
        columns=FEATURE_ORDER_WEEKLY, data=[FIRST_FEATURE_VALUES_WEEKLY]
    )

    difference = difference_between_dataframes_without_duplicates_ignoring_column_order(
        [
            first_feature_values,
            features_from_db.head(n=1),
        ]
    )
    assert list(difference.values) == []
