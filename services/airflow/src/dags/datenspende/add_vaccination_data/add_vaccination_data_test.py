from postgres_helpers import DBContext
from src.lib.test_helpers import run_task_with_url, run_task
from .add_vaccination_data import (
    add_vaccination_data_to_homogenized_feature_table,
    load_and_transform_vaccination_data_for_each_user,
)

import pandas.api.types as ptypes


LOCAL_DOWNLOAD_URL = "http://static-files/thryve/exportStudy_reduced.7z"


def test_vaccination_data_has_correct_format_and_types(pg_context):
    run_task_with_url(
        "datenspende_surveys_v2", "gather_data_from_thryve", LOCAL_DOWNLOAD_URL
    )
    run_task("datenspende_surveys_v2", "extract_features_from_weekly_tasks")
    run_task("datenspende_surveys_v2", "extract_features_from_one_off_answers")

    vaccination_data = load_and_transform_vaccination_data_for_each_user()

    assert list(vaccination_data.columns) == [
        "user_id",
        "test_week_start",
        "administered_vaccine_doses",
        "days_since_last_dose",
    ]

    # columns containing of int and None are object in pandas
    assert ptypes.is_object_dtype(vaccination_data["days_since_last_dose"])
    print(vaccination_data["administered_vaccine_doses"])


def test_vaccination_data_task_runs_without_errors(pg_context: DBContext):
    run_task_with_url(
        "datenspende_surveys_v2", "gather_data_from_thryve", LOCAL_DOWNLOAD_URL
    )
    run_task("datenspende_surveys_v2", "extract_features_from_weekly_tasks")
    run_task("datenspende_surveys_v2", "extract_features_from_one_off_answers")
    add_vaccination_data_to_homogenized_feature_table()
