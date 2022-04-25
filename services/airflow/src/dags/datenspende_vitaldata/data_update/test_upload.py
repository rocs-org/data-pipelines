import os

import ramda as R

from postgres_helpers import DBContext, query_all_elements
from .download import download
from .transform import transform
from .upload import upload

URL = "http://static-files/thryve/export.7z"


def test_upload_writes_all_dataframe_to_database(pg_context: DBContext):
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

    assert (
        number_of_elements_returned_from(pg_context)("SELECT * FROM datenspende.users")
        == 168
    )
    assert (
        number_of_elements_returned_from(pg_context)(
            "SELECT * FROM datenspende.vitaldata"
        )
        == 35
    )


@R.curry
def number_of_elements_returned_from(pg_context):
    return R.pipe(query_all_elements(pg_context), len)
