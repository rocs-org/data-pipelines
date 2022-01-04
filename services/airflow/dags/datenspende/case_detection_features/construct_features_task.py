import ramda as R

from dags.datenspende.case_detection_features.transform import (
    restructure_features,
    collect_feature_names,
)
from dags.datenspende.case_detection_features.load import (
    upload_all_dataframes,
)
from dags.datenspende.case_detection_features.extract import load_test_and_symptoms_data
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present


def extract_features_task(
    questionnaire_id: int,
    questions: dict,
    feature_table: str,
    feature_description_table: str,
    **kwargs
):
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        load_test_and_symptoms_data(questionnaire_id, questions),
        R.apply_spec(
            {
                feature_table: {
                    "df": restructure_features(questionnaire_id, questions),
                    "constraints": R.always(["unique_answers"]),
                },
                feature_description_table: {
                    "df": collect_feature_names(questions),
                    "constraints": R.always(["id_unique"]),
                },
            }
        ),
        lambda d: [(key, val["constraints"], val["df"]) for key, val in d.items()],
        upload_all_dataframes("datenspende_derivatives"),
        R.prop("credentials"),
    )(kwargs)
