import pandas as pd

from dags.datenspende.case_detection_features.parameters import ONE_OFF_QUESTIONS
from dags.datenspende.case_detection_features.extract.extract_survey_data import (
    load_test_and_symptoms_data,
)
from dags.helpers.test_helpers import run_task_with_url


def test_extract_features_from_tests_and_symptoms_questionnarie():

    run_task_with_url(
        "datenspende",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )

    loader = load_test_and_symptoms_data(10, ONE_OFF_QUESTIONS)

    features = loader("")

    assert isinstance(features, pd.DataFrame)
    assert len(features.index) == 438
    assert list(features.iloc[0].values) == [
        8382,
        127,
        774,
        "Welches Geschlecht haben Sie?",
        "Männlich",
    ]
