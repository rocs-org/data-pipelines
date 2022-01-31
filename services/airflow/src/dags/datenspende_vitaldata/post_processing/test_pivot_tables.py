from database import DBContext, query_all_elements
import os
import ramda as R
from ..data_update.download import download
from ..data_update.transform import transform
from ..data_update.upload import upload
from .pivot_tables import create_pivot_table, PIVOT_TARGETS


def setup_vitaldata_in_db():
    URL = "http://static-files/thryve/export.7z"
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


def test_pivot_vitaldata_creates_correct_tables(db_context: DBContext):
    setup_vitaldata_in_db()
    for vitalid, vitaltype, datatype in PIVOT_TARGETS:
        create_pivot_table(db_context, vitalid, vitaltype, datatype)

    steps_data = query_all_elements(
        db_context, "SELECT * FROM datenspende_derivatives.steps_ct;"
    )
    resting_heartrate_data = query_all_elements(
        db_context, "SELECT * FROM datenspende_derivatives.resting_heartrate_ct;"
    )
    assert steps_data[-1] == (200, 3600, None, None, None, None, None, None, None)
    assert resting_heartrate_data[0] == (100, None, None, 50, 50, 50, None, None, None)
