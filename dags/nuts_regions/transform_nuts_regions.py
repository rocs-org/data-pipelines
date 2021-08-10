import pandas as pd


def transform_nuts_regions(raw_regions_data: pd.DataFrame) -> pd.DataFrame:
    transformed = raw_regions_data

    transformed["name"] = transformed[
        ["Country", "NUTS level 1", "NUTS level 2", "NUTS level 3"]
    ].apply(select_level_name, axis=1)

    return (
        transformed[["Code 2021", "NUTS level", "name", "Country order"]]
        .rename(
            columns={
                "Code 2021": "geo",
                "NUTS level": "level",
                "Country order": "country_id",
            },
        )
        .dropna(axis="rows", how="any")
        .astype({"geo": str, "level": int, "name": str, "country_id": int})
    )


def select_level_name(row) -> str:
    if type(row["Country"]) == str:
        return row["Country"]
    elif type(row["NUTS level 1"]) == str:
        return row["NUTS level 1"]
    elif type(row["NUTS level 2"]) == str:
        return row["NUTS level 2"]
    elif type(row["NUTS level 3"]) == str:
        return row["NUTS level 3"]
    return ""
