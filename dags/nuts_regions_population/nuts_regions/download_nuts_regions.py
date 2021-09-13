import pandas as pd


def download_nuts_regions(url):
    return pd.read_excel(url, sheet_name="NUTS & SR 2021")
