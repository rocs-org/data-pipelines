import pandas as pd


def download_hospitalizations(url):
    return pd.read_csv(url)
