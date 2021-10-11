import os
from .download import download
from .transform import transform

URL = "http://static-files/thryve/exportStudy.7z"


def test_transform_transforms_column_names_to_snake_case():
    downloads = download(
        {
            "username": os.environ["THRYVE_FTP_USER"],
            "password": os.environ["THRYVE_FTP_PASSWORD"],
            "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
        },
        URL,
    )
    check_transformed_columns(
        "answers",
        [
            "id",
            "user_id",
            "questionnaire_session",
            "study",
            "questionnaire",
            "question",
            "order_id",
            "created_at",
            "element",
            "answer_text",
        ],
        downloads,
    )
    check_transformed_columns(
        "choice",
        [
            "element",
            "question",
            "choice_id",
            "text",
        ],
        downloads,
    )
    check_transformed_columns(
        "questionnaires",
        ["id", "name", "description", "hour_of_day_to_answer", "expiration_in_minutes"],
        downloads,
    )
    check_transformed_columns(
        "questionnaire_session",
        [
            "id",
            "user_id",
            "study",
            "questionnaire",
            "session_run",
            "expiration_timestamp",
            "created_at",
            "completed_at",
        ],
        downloads,
    )
    check_transformed_columns(
        "users",
        [
            "customer",
            "plz",
            "salutation",
            "birth_date",
            "weight",
            "height",
            "creation_timestamp",
            "source",
        ],
        downloads,
    )


def check_transformed_columns(key, columns, df):
    assert list(transform(df[key]).columns) == columns
