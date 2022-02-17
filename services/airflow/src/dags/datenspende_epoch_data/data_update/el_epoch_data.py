import os

import ramda as R

from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from .extract import extract
from .load import load

EPOCH_URL = os.environ["THRYVE_FTP_URL"] + "exportEpoch.7z"
EPOCH_TABLE = "vital_data_epoch"

VITAL_DATA_UPDATE_ARGS = [EPOCH_URL, EPOCH_TABLE]


def el_epoch_data(url: str, table: str, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("CLICKHOUSE_DB"),
        lambda *args: extract(
            {
                "username": os.environ["THRYVE_FTP_USER"],
                "password": os.environ["THRYVE_FTP_PASSWORD"],
                "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
            },
            url,
        ),
        R.tap(lambda filelist: [print(filename) for filename, _ in filelist]),
        load(table),
    )(kwargs)
