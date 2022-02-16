from database import DBContext, query_all_elements
import os
import ramda as R
from .download import download
from .transform import transform
from .upload import upload

URL = "http://static-files/thryve/exportStudy.7z"


def test_datenspende_upload_writes_all_dataframe_to_database(pg_context: DBContext):
    R.pipe(
        download(
            {
                "username": os.environ["THRYVE_FTP_USER"],
                "password": os.environ["THRYVE_FTP_PASSWORD"],
                "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
            }
        ),
        R.map(lambda item: (item[0], transform(item[1]))),
        upload("datenspende"),
    )(URL)

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.answers"
        )
        == 5765
    )

    assert (
        number_of_elements_returned_from(pg_context)("SELECT * FROM datenspende.choice")
        == 760
    )

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.questionnaires"
        )
        == 5
    )

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.questionnaire_session"
        )
        == 347
    )

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.questions"
        )
        == 90
    )

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.question_to_questionnaire"
        )
        == 91
    )

    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.study_overview"
        )
        == 2
    )
    assert (
        number_of_elements_returned_from(pg_context)("SELECT * FROM datenspende.users")
        == 168
    )


@R.curry
def number_of_elements_returned_from(pg_context):
    return R.pipe(query_all_elements(pg_context), len)
