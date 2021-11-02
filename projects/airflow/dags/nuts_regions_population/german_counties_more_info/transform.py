import pandas as pd
import ramda as R
import datetime
from returns.curry import curry


def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    return R.pipe(
        drop_rows_with_nan_in_nth_col(4),
        rename_columns_with_mapping(COLUMN_MAPPING),
        add_data_column(datetime.datetime(2019, 12, 31)),
        set_column_types_to(COLUMN_TYPES),
    )(dataframe)


@curry
def drop_rows_with_nan_in_nth_col(col: int, df: pd.DataFrame) -> pd.DataFrame:
    return df[pd.notnull(df[col])]


@curry
def rename_columns_with_mapping(mapping: dict, df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(axis="columns", mapper=mapping)


@curry
def add_data_column(date: datetime.datetime, df: pd.DataFrame) -> pd.DataFrame:
    df["year"] = date
    return df


@curry
def set_column_types_to(type_def: dict, df: pd.DataFrame) -> pd.DataFrame:
    return df.astype(type_def)


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

COLUMN_TYPES = {
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
