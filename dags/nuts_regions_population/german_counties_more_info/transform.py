import pandas as pd
import ramda as R
import datetime
from returns.curry import curry


def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    return R.pipe(
        lambda df: df[pd.notnull(df[4])],
        lambda df: df.rename(axis="columns", mapper=COLUMN_MAPPING),
        add_data_column(datetime.datetime(2019, 12, 31)),
        lambda df: df.astype(
            {
                "german_id": int,
                "regional_identifier": str,
                "municipality": str,
                "nuts": str,
                "area": float,
                "population": int,
                "m_population": int,
                "f_population": int,
                "population_density": float,
                "year": "datetime64[ns]",
            }
        ),
    )(dataframe)


@curry
def add_data_column(date: datetime.datetime, df: pd.DataFrame) -> pd.DataFrame:
    df["year"] = date
    return df


COLUMN_MAPPING = {
    1: "german_id",
    "2": "regional_identifier",
    3: "municipality",
    4: "nuts",
    5: "area",
    6: "population",
    7: "m_population",
    8: "f_population",
    9: "population_density",
}
