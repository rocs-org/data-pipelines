import pandas as pd
import ramda as R
from returns.curry import curry
from database import create_db_context, query_all_elements

INDICATORS = r"sex,unit,age,geo\time"


def transform(raw: pd.DataFrame) -> pd.DataFrame:
    return R.pipe(
        lambda df: df.set_index([INDICATORS]),
        lambda df: df.unstack(),
        lambda series: pd.DataFrame(series, columns=["number"]),
        lambda df: df.reset_index(),
        lambda df: df.rename(columns={"level_0": "year"}),
        set_nth_indicator_as_column("nuts", 3),
        set_nth_indicator_as_column("agegroup", 2),
        set_nth_indicator_as_column("sex", 0),
        lambda df: df.drop(columns=[INDICATORS]),
        filter_for_existing_regions,
        R.tap(print),
        split_numbers_into_int_and_data_flag,
        lambda df: df.dropna(axis=0, how="any"),
        lambda df: df.astype(
            {
                "year": int,
                "number": int,
                "nuts": str,
                "agegroup": str,
                "sex": str,
                "data_quality_flags": str,
            }
        ),
    )(raw)


@curry
def set_nth_indicator_as_column(
    column_name, indicator_index, df: pd.DataFrame
) -> pd.DataFrame:
    try:
        df[column_name] = df[INDICATORS].apply(
            lambda val: R.pipe(R.split(","), R.nth(indicator_index))(val)
        )
    except Exception as e:
        print(column_name, indicator_index)
        raise e
    return df


def split_numbers_into_int_and_data_flag(df: pd.DataFrame) -> pd.DataFrame:
    df[["number", "data_quality_flags"]] = df["number"].apply(
        split_number_into_int_and_data_flag
    )
    return df


def filter_for_existing_regions(df: pd.DataFrame) -> pd.DataFrame:
    db_context = create_db_context()
    regions = pd.DataFrame(
        columns=["nuts"],
        data=query_all_elements(db_context, "SELECT (geo) FROM censusdata.nuts;"),
    )
    return pd.merge(regions, df, on=["nuts"])


def split_number_into_int_and_data_flag(str_number: str or int) -> [float, str]:

    if str_number == ":":
        res = [float("nan"), ""]
    elif str_number == ": ":
        res = [float("nan"), ""]
    elif type(str_number) is int:
        res = [float(str_number), ""]
    elif " " in str_number:
        splits = str_number.split()
        if len(splits) == 2:
            res = [float(splits[0]), insert_full_text_flags(splits[1])]
        else:
            res = [float(splits[0]), ""]
    else:
        res = [float(str_number), ""]
    return pd.Series(res)


def insert_full_text_flags(short_flags: str) -> str:
    return R.reduce(
        lambda full_text_flags, flag: R.concat(
            full_text_flags,
            DATA_QUALITY_FLAGS[flag] + ",",
        ),
        "",
        list(short_flags),
    )


def to_int(string: str) -> int:
    return int(string)


DATA_QUALITY_FLAGS = {
    "b": "break in time series",
    "c": "confidential",
    "d": "definition differs, see metadata",
    "e": "estimated",
    "f": "forecast",
    "n": "not significant",
    "p": "provisional",
    "r": "revised",
    "s": "Eurostat estimate",
    "u": "low reliability",
    "z": "not applicable",
    "": "",
}
