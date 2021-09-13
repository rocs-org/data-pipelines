import pandas as pd


def download(url: str) -> pd.DataFrame:
    return pd.read_excel(url, sheet_name="Kreisfreie Städte u. Landkreise", header=5)
