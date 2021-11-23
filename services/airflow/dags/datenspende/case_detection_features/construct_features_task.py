import ramda as R

from dags.datenspende.case_detection_features.transform import (
    restructure_features,
    collect_feature_names,
)
from dags.datenspende.case_detection_features.load import (
    upload_all_dataframes,
)
from dags.datenspende.case_detection_features.extract import load_test_and_symptoms_data


def extract_features_task(
    questionnaire_id: int,
    questions: dict,
    feature_table: str,
    feature_description_table: str,
):
    return R.pipe(
        load_test_and_symptoms_data(questionnaire_id, questions),
        R.apply_spec(
            {
                feature_table: restructure_features(questions),
                feature_description_table: collect_feature_names(questions),
            }
        ),
        lambda d: list(d.items()),
        upload_all_dataframes("datenspende_derivatives"),
        R.prop("credentials"),
    )("")
