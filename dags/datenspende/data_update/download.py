import polars
import requests
import py7zr
import polars as po
from polars.datatypes import Utf8, Int64
import ramda as R
from typing import List, Tuple
from returns.curry import curry
from requests.auth import HTTPBasicAuth


DataList = List[Tuple[str, polars.DataFrame]]


@R.curry
def download(access_config: dict, url: str) -> DataList:
    return R.pipe(
        download_file(access_config),
        unzip_file(access_config),
        load_files,
        map_dict_to_list,
    )(url)


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


def load_files(*_):
    return {
        "users": po.read_csv("usersAll.csv"),
        "answers": po.read_csv(
            "answers.csv",
            dtype={
                "id": Int64,
                "user": Int64,
                "questionnaireSession": Int64,
                "study": Int64,
                "questionnaire": Int64,
                "question": Int64,
                "order": Int64,
                "createdAt": Int64,
                "element": Int64,
                "value": Utf8,
            },
        ),
        "choice": po.read_csv("choice.csv"),
        "questionnaires": po.read_csv(
            "questionnaires.csv",
            dtype={
                "id": Int64,
                "name": Utf8,
                "description": Utf8,
                "hourOfDayToAnswer": Int64,
                "expirationInMinutes": Int64,
            },
        ),
        "questionnaire_session": po.read_csv("questionnaireSession.csv"),
        "questions": po.read_csv("questions.csv"),
        "question_to_questionnaire": po.read_csv("questionToquestionnaire.csv"),
        "study_overview": po.read_csv("studyOverview.csv"),
    }


def map_dict_to_list(d: dict) -> list:
    return [(key, value) for key, value in d.items()]