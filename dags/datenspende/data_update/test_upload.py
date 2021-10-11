from dags.database import DBContext, create_db_context, query_all_elements
import os
import ramda as R
from .download import download
from .transform import transform
from .upload import upload

URL = "http://static-files/thryve/exportStudy.7z"


def test_upload_writes_all_dataframe_to_database(db_context: DBContext):
    R.pipe(
        download(
            {
                "username": os.environ["THRYVE_FTP_USER"],
                "password": os.environ["THRYVE_FTP_PASSWORD"],
                "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
            }
        ),
        lambda data: {key: transform(dataframe) for key, dataframe in data.items()},
        upload("datenspende"),
    )(URL)

    db_context = create_db_context()
    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.answers"
        )
        == 5126
    )

    assert (
        number_of_elements_returned_from(db_context)("SELECT * FROM datenspende.choice")
        == 658
    )

    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.questionnaires"
        )
        == 4
    )

    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.questionnaire_session"
        )
        == 521
    )

    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.questions"
        )
        == 93
    )

    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.question_to_questionnaire"
        )
        == 94
    )

    assert (
        number_of_elements_returned_from(db_context)(
            "SELECT * FROM datenspende.study_overview"
        )
        == 2
    )
    assert (
        number_of_elements_returned_from(db_context)("SELECT * FROM datenspende.users")
        == 119
    )


@R.curry
def number_of_elements_returned_from(db_context):
    return R.pipe(query_all_elements(db_context), len)
