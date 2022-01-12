import pandas as pd


def download(url: str) -> pd.DataFrame:
    return pd.read_csv(url, sep=";")
