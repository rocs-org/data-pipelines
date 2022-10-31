import os

ONETIME_VITAL_DATA_UPDATE_ARGS = [
    os.environ["THRYVE_FTP_URL"] + "fill_gaps.7z",
    "datenspende",
]


def go_msg():
    print("~~~~~~~~~ DAG GO ~~~~~~~~~")
