import pandas as pd

from download_hospitalizations import download_hospitalizations
from transform_dataframe import transform_dataframe

URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
url = "http://static-files/static/hospitalizations.csv"
Url = "../../files/static/hospitalizations.csv"
df = download_hospitalizations(Url)
transform = transform_dataframe(df)
print(len(transform))
print(transform.head(5))
print(transform.iloc[0])
print(transform.info())