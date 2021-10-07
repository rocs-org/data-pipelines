import requests
import py7zr
import pandas as pd
import ramda as R
from typing import Dict
from returns.curry import curry
from requests.auth import HTTPBasicAuth


def download(access_config: dict, url: str) -> Dict[str, pd.DataFrame]:
    return R.pipe(download_file(access_config), unzip_file(access_config), load_files)(
        url
    )


@curry
def download_file(access_config: dict, url):
    local_filename = "exportStudy.7z"
    with requests.get(
        url,
        stream=True,
        auth=HTTPBasicAuth(access_config["username"], access_config["password"]),
    ) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


@curry
def unzip_file(access_config: dict, filename: str) -> None:
    with py7zr.SevenZipFile(
        filename, "r", password=access_config["zip_password"]
    ) as file:
        file.extractall()


def load_files(*args):
    return {
        "answers": pd.read_csv("answers.csv"),
        "choice": pd.read_csv("choice.csv"),
        "questionnaires": pd.read_csv("questionnaires.csv"),
        "questionnaire_session": pd.read_csv("questionnaireSession.csv"),
        "questions": pd.read_csv("questions.csv"),
        "questions_to_questionnaire": pd.read_csv("questionToquestionnaire.csv"),
        "study_overview": pd.read_csv("studyOverview.csv"),
        "users_all": pd.read_csv("usersAll.csv"),
    }
