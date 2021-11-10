from database import DBContext, create_db_context, query_all_elements
import os
import ramda as R
from .download import download
from .transform import transform
from .upload import upload

URL = "http://static-files/thryve/export.7z"


def test_upload_writes_all_dataframe_to_database(db_context: DBContext):
    R.pipe(
        download(
            {
                "username": os.environ["THRYVE_FTP_USER"],
                "password": os.environ["THRYVE_FTP_PASSWORD"],
                "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
            }
        ),
        R.map(lambda item: (item[0], item[1], transform(item[2]))),
        upload("datenspende"),
    )(URL)

    db_context = create_db_context()

    assert (
        number_of_elements_returned_from(db_context)("SELECT * FROM datenspende.users")
        == 168
    )
    assert (
        number_of_elements_returned_from(db_context)("SELECT * FROM datenspende.vitaldata")
        == 10
    )


@R.curry
def number_of_elements_returned_from(db_context):
    return R.pipe(query_all_elements(db_context), len)
