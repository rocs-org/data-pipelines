import polars as po
from typing import List
from returns.curry import curry
from datetime import timedelta, date
from dags.database import create_db_context, teardown_db_context, query_all_elements


def load_counties_info(_):
    db_context = create_db_context()
    data = query_all_elements(
        db_context,
        """
            SELECT
                german_id,
                nuts,
                population
            FROM
                censusdata.german_counties_info
        """,
    )
    teardown_db_context(db_context)
    germanid, nuts3, population = zip(*data)
    return po.DataFrame(
        {
            "germanid": germanid,
            "nuts3": nuts3,
            "population": population,
        }
    )


def load_cases_data(_):
    db_context = create_db_context()
    data = query_all_elements(
        db_context,
        """
            SELECT
                countyid,
                agegroup,
                sex,
                date_cet,
                ref_date_cet,
                ref_date_is_symptom_onset,
                is_new_case,
                is_new_death,
                is_new_recovered,
                new_cases,
                new_deaths,
                new_recovereds
            FROM
                coronacases.german_counties_more_info
        """,
    )
    teardown_db_context(db_context)
    (
        countyid,
        agegroup,
        sex,
        date_cet,
        ref_date_cet,
        ref_date_is_symptom_onset,
        is_new_case,
        is_new_death,
        is_new_recovered,
        new_cases,
        new_deaths,
        new_recovereds,
    ) = zip(*data)
    return po.DataFrame(
        {
            "IdLandkreis": countyid,
            "Altersgruppe": agegroup,
            "Geschlecht": sex,
            "Meldedatum": date_cet,
            "Refdatum": ref_date_cet,
            "IstErkrankungsbeginn": ref_date_is_symptom_onset,
            "NeuerFall": is_new_case,
            "NeuerTodesfall": is_new_death,
            "NeuGenesen": is_new_recovered,
            "AnzahlFall": new_cases,
            "AnzahlTodesfall": new_deaths,
            "AnzahlGenesen": new_recovereds,
        }
    )


def calculate_incidence(
    population_data: po.DataFrame, cases_data: po.DataFrame
) -> po.DataFrame:
    """
    calculate incidence data from cencsus and case data.
    Authored by @benmaier
    """

    keys = ["IdLandkreis", "Meldedatum"]

    grouped = aggregate_df_on_keys(keys, cases_data)

    grouped["Meldedatum"] = grouped["Meldedatum"].cast(po.Date32)

    # get landkreis ids
    lk = grouped["IdLandkreis"].unique().sort().to_list()

    continuous_dates = get_continuous_dates("Meldedatum", cases_data)

    # create full key dataset ( len(dates) * len(LK) )
    ids = []
    [ids.extend([i] * len(continuous_dates)) for i in lk]
    continuous_dates *= len(lk)

    dates = po.DataFrame({"date": continuous_dates, "id": ids})
    dates["date"] = dates["date"].cast(po.Date32)

    # create non-existing entries and fill cases with zeros
    continuous = grouped.join(
        dates, left_on=keys, right_on=["id", "date"], how="outer"
    ).sort(by=keys)

    continuous = continuous.join(
        population_data, left_on="IdLandkreis", right_on="germanid", how="left"
    )

    # add zeros
    mask = continuous["new_cases"].is_null()
    continuous[mask, "new_cases"] = 0
    continuous[mask, "new_deaths"] = 0

    new = None

    # add rolling sum for every landkreis and date

    for i in lk:

        this_lk = continuous[continuous["IdLandkreis"] == i]
        roll = this_lk["new_cases"].rolling_sum(7)

        this_lk["new_cases_last_7d"] = roll
        if new is None:
            new = this_lk
        else:
            new = new.vstack(this_lk)

    # these are columns over which stuff will be aggregated
    agg_cols = [
        po.col("new_cases").sum().alias("new_cases"),
        po.col("new_cases_last_7d").sum().alias("new_cases_last_7d"),
        po.col("new_deaths").sum().alias("new_deaths"),
        po.col("population").sum().alias("population"),
        po.col("state").first().alias("state"),
    ]

    # add state data
    new["state"] = [i // 1000 for i in new["IdLandkreis"]]
    new["state"] = new["state"]

    # get berlin and add it as a Landkreis
    berlin = (
        new.filter(po.col("state") == 11)
        .groupby("Meldedatum")
        .agg(agg_cols)
        .sort("Meldedatum")
    )

    berlin["nuts3"] = ["DE300" for _ in berlin["Meldedatum"]]
    berlin["IdLandkreis"] = [11000 for _ in berlin["Meldedatum"]]
    berlin["IdLandkreis"] = berlin["IdLandkreis"]
    berlin = berlin[new.columns]

    # construct location_level 4 (landkreise and berliner bezirke)
    landkreise_and_berliner_bezirke = new.clone()
    landkreise_and_berliner_bezirke["id"] = landkreise_and_berliner_bezirke[
        "IdLandkreis"
    ]
    landkreise_and_berliner_bezirke["location_level"] = [
        4 for _ in landkreise_and_berliner_bezirke["IdLandkreis"]
    ]
    landkreise_and_berliner_bezirke = landkreise_and_berliner_bezirke.drop(
        "IdLandkreis"
    )

    # construct location_level 3 (landkreise)
    landkreise = new.filter(po.col("state") != 11)
    landkreise = landkreise.vstack(berlin).sort(keys)
    landkreise["id"] = landkreise["IdLandkreis"]
    landkreise = landkreise.drop("IdLandkreis")
    landkreise["location_level"] = [3 for _ in landkreise["id"]]

    # construct location_level 1 (states)
    states = (
        new.groupby(["state", "Meldedatum"])
        .agg(agg_cols[:-1])
        .sort(["state", "Meldedatum"])
    )
    states["id"] = states["state"]
    states["location_level"] = [1 for _ in states["id"]]
    states = states.lazy().with_columns([po.lit(None).alias("nuts3")]).collect()

    # construct location_level 0 (germany)
    germany = new.groupby("Meldedatum").agg(agg_cols).sort("Meldedatum")
    germany["id"] = [0 for _ in germany["Meldedatum"]]
    germany["location_level"] = [0 for _ in germany["Meldedatum"]]
    germany = germany.lazy().with_columns([po.lit(None).alias("state")]).collect()
    germany = germany.lazy().with_columns([po.lit(None).alias("nuts3")]).collect()
    germany["state"] = germany["state"].cast(po.Int64)

    # function to compute 7d incidence
    def add_7d_per_100k(df):
        df["incidence_7d_per_100k"] = df["new_cases_last_7d"] / df["population"] * 1e5
        # df['offset_incidence_7d_per_100k'] = (df['new_cases_last_7d']+1) / df['population'] * 1e5

    # add 7d incidence and concatenate
    dfall = None

    for cases_data in [
        landkreise_and_berliner_bezirke,
        landkreise,
        states,
        germany,
    ]:
        add_7d_per_100k(cases_data)
        cases_data["nuts3"] = cases_data["nuts3"].cast(po.Utf8)
        cases_data = cases_data.sort(by=["id", "Meldedatum"])
        if dfall is None:
            dfall = cases_data
        else:
            cases_data = cases_data[dfall.columns]
            dfall = dfall.vstack(cases_data)

    dfall["date_of_report"] = dfall["Meldedatum"]
    dfall = dfall.drop("Meldedatum")

    dfall["location_id"] = dfall["id"]
    dfall = dfall.drop("id")

    dfall["new_cases"] = dfall["new_cases"].cast(po.Int32)
    dfall["new_deaths"] = dfall["new_deaths"].cast(po.Int32)
    dfall["population"] = dfall["population"].cast(po.UInt32)
    dfall["new_cases_last_7d"] = dfall["new_cases_last_7d"].cast(po.Int32)
    dfall["state"] = dfall["state"].cast(po.UInt8)
    dfall["location_id"] = dfall["location_id"].cast(po.UInt16)
    dfall["location_level"] = dfall["location_level"].cast(po.UInt8)
    dfall["incidence_7d_per_100k"] = dfall["incidence_7d_per_100k"].cast(po.Float32)

    dfall["date_of_report"] = dfall["date_of_report"].apply(
        lambda d: date32_to_datetime(d)
    )

    return dfall[
        [
            "location_id",
            "location_level",
            "date_of_report",
            "new_cases",
            "new_cases_last_7d",
            "incidence_7d_per_100k",
            "new_deaths",
            "nuts3",
            "population",
            "state",
        ]
    ]


@curry
def aggregate_df_on_keys(keys: List[str], df: po.DataFrame) -> po.DataFrame:
    return (
        df.groupby(keys)
        .agg(
            [
                po.col("AnzahlFall").sum().alias("new_cases"),
                po.col("AnzahlTodesfall").sum().alias("new_deaths"),
            ]
        )
        .sort(by=keys)
    )


@curry
def get_continuous_dates(column_id: str, df: po.DataFrame):
    min_date = df[column_id].cast(po.Date32).min()
    max_date = df[column_id].cast(po.Date32).max()
    return list(range(min_date, max_date + 1))


def date32_to_datetime(date32):
    return date(1970, 1, 1) + timedelta(days=date32)
