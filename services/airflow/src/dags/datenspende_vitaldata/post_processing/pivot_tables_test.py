from postgres_helpers import DBContext, query_all_elements
import os
import ramda as R
from ..data_update.download import download
from ..data_update.transform import transform
from ..data_update.upload import upload
from .pivot_tables import create_pivot_table, PIVOT_TARGETS


def setup_vitaldata_in_db(URL="http://static-files/thryve/export.7z"):
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


def test_pivot_vitaldata_creates_correct_tables(pg_context: DBContext):
    setup_vitaldata_in_db()
    for vitalid, vitaltype, datatype in PIVOT_TARGETS[0]:
        create_pivot_table(pg_context, vitalid, vitaltype, datatype)

    steps_data = query_all_elements(
        pg_context,
        "SELECT * FROM datenspende_derivatives.steps_ct WHERE user_id != 236;",
    )
    resting_heartrate_data = query_all_elements(
        pg_context, "SELECT * FROM datenspende_derivatives.resting_heartrate_ct;"
    )
    print(steps_data[-1])
    print(resting_heartrate_data[0])
    assert steps_data[-1] == (200, 3600, None, None, None, None, 4601, 4601, 4601)
    assert resting_heartrate_data[0] == (100, None, None, 50, 50, 50, None, None, None)
