import pandas as pd


def download_data(url: str) -> pd.DataFrame:
    return pd.read_csv(url, sep="\t")
