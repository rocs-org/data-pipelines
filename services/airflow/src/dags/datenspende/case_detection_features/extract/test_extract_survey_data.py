import pandas as pd
import pytest
from postgres_helpers import DBContext
from src.dags.datenspende.case_detection_features.parameters import (
    ONE_OFF_QUESTIONS,
    WEEKLY_QUESTIONS,
    ONE_OFF_QUESTIONNAIRE,
    WEEKLY_QUESTIONNAIRE,
)
from src.dags.datenspende.case_detection_features.extract.extract_survey_data import (
    load_test_and_symptoms_data,
)
from src.lib.test_helpers import run_task_with_url


def test_extract_features_from_tests_and_symptoms_questionnarie(prepared_db):

    loader = load_test_and_symptoms_data(ONE_OFF_QUESTIONNAIRE, ONE_OFF_QUESTIONS)

    features = loader("")

    assert isinstance(features, pd.DataFrame)
    assert len(features.index) == 474
    assert (
        len(features.groupby(["user_id"])) == 91
    )  # Individuals that reported a (positive or negative) test result
    print(list(features.iloc[0].values))
    assert list(features.iloc[0].values) == [
        8382,
        1394,
        121,
        728,
        "Sind sie bereits vollständig gegen das Coronavirus geimpft?",
        "Ja",
    ]


def test_extract_features_from_weekly_questionnarie(prepared_db):

    loader = load_test_and_symptoms_data(WEEKLY_QUESTIONNAIRE, WEEKLY_QUESTIONS)

    features = loader("")

    assert isinstance(features, pd.DataFrame)
    assert (
        len(features.groupby(["user_id"])) == 7
    )  # Individuals that reported a (positive or negative) test result

    assert list(features.iloc[0].values) == [
        224410,
        1420,
        74,
        349,
        "Wie groß sind Sie, wenn Sie keine Schuhe tragen (in cm)?",
        "170cm bis 174cm",
    ]


@pytest.fixture
def prepared_db(pg_context: DBContext):
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )
    return pg_context
