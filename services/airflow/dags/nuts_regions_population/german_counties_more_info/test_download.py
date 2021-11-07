from .download import download

URL = "http://static-files/static/04-kreise.xlsx"


def test_download_loads_data_correctly():
    data = download(URL)
    print(data.head())
    assert list(data.columns) == COLUMNS
    assert list(data.iloc[3]) == THIRD_ROW


COLUMNS = [
    1,
    "2",
    3,
    4,
    5,
    6,
    7,
    8,
    9,
]

THIRD_ROW = [
    "01002",
    "Kreisfreie Stadt",
    "Kiel, Landeshauptstadt",
    "DEF02",
    118.65,
    246794.0,
    120198.0,
    126596.0,
    2080.0,
]
