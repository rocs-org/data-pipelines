import pandas as pd

from .download_population_data import download_data

URL = "http://static-files/static/demo_r_pjangrp3.tsv"


def test_download_data_returns_df_with_data():
    df = download_data(URL)
    assert type(df) == pd.DataFrame
    print(df.head())
    assert list(df.columns) == [
        r"sex,unit,age,geo\time",
        "2020 ",
        "2019 ",
        "2018 ",
        "2017 ",
        "2016 ",
        "2015 ",
        "2014",
    ]
