import pandas as pd
from isoweek import Week
from datetime import datetime
from numpy import datetime64 as d64
from numpy import timedelta64 as td64


def transform_dataframe(df):
    population = 83020000  # TODO replace hardcoded population with reference that will be updated
    df = df[df['country'] == 'Germany']
    df = df[df['indicator'] == 'Weekly new hospital admissions per 100k']

    new_hospitalizations = [int(_) for _ in (df['value'] * population / 1e5).astype(int).to_list()]
    new_hospitalizations_per_100k = [float(_) for _ in df['value'].to_list()]
    year = [int(ywk.split("-W")[0]) for ywk in df['year_week']]
    calendar_week = [int(ywk.split("-W")[1]) for ywk in df['year_week']]
    year_week = df['year_week'].to_list()
    date_begin = [Week.fromstring(ywk).monday() for ywk in df['year_week']]
    date_end = [Week.fromstring(ywk).sunday() for ywk in df['year_week']]
    days_since_jan1_begin = [int((d64(str(d)) - d64("2020-01-01")) / td64(1, 'D')) for d in date_begin]
    days_since_jan1_end = [int((d64(str(d)) - d64("2020-01-01")) / td64(1, 'D')) for d in date_end]
    date_updated = [datetime.now() for i in range(len(year))]
    d = {
        "new_hospitalizations": new_hospitalizations,
        "new_hospitalizations_per_100k": new_hospitalizations_per_100k,
        "year": year,
        "calendar_week": calendar_week,
        "year_week": year_week,
        "date_begin": date_begin,
        "date_end": date_end,
        "days_since_jan1_begin": days_since_jan1_begin,
        "days_since_jan1_end": days_since_jan1_end,
        "date_updated": date_updated
    }
    df_new = pd.DataFrame(d)
    return df_new
