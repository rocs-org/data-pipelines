import os
import ramda as R
from .download import download
from .transform import transform
from .upload import upload
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present


DATA_UPDATE_ARGS = [os.environ["THRYVE_FTP_URL"] + "exportStudy.7z", "datenspende"]


def data_update_etl(url: str, schema: str, **kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download(
            {
                "username": os.environ["THRYVE_FTP_USER"],
                "password": os.environ["THRYVE_FTP_PASSWORD"],
                "zip_password": os.environ["THRYVE_ZIP_PASSWORD"],
            },
            url,
        ),
        lambda data: {key: transform(dataframe) for key, dataframe in data.items()},
        upload(schema),
    )(kwargs)
