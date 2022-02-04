import numpy as np
import pandas as pd
import polars as po
from datetime import date, timedelta, datetime
from .write_dataframe_to_postgres import (
    _build_insert_query,
    _upsert_column_action,
    _build_upsert_query,
    connect_to_db_and_insert_pandas_dataframe,
    connect_to_db_and_truncate_insert_pandas_dataframe,
    connect_to_db_and_insert_polars_dataframe,
    connect_to_db_and_upsert_pandas_dataframe,
)
from database import DBContext, query_all_elements, with_db_context


def test_insert_dataframe_to_postgres_works_with_polars_dataframe(
    db_context: DBContext,
):
    def date32_to_datetime(date32):
        return date(1970, 1, 1) + timedelta(days=date32)

    df = DATA3

    df["date_of_report"] = df["date_of_report"].apply(lambda d: date32_to_datetime(d))

    connect_to_db_and_insert_polars_dataframe(
        schema="coronacases", table="german_counties_incidence", data=df
    )

    res = with_db_context(
        query_all_elements, "SELECT * FROM coronacases.german_counties_incidence;"
    )
    assert len(res) == 2
    assert res[1] == (
        1,
        None,
        datetime(1970, 1, 2, 0, 0),
        None,
        None,
        1.0,
        None,
        None,
        None,
        None,
    )

    assert np.isnan(res[0][5])


def test_insert_dataframe_to_postgres_writes_data_correctly(db_context: DBContext):

    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA1
    )
    res = with_db_context(query_all_elements, "SELECT * FROM censusdata.nuts;")

    assert len(res) == 2
    assert res[0] == (0, "DE", "Deutschland", 0)


def test_insert_dataframe_to_postgres_does_not_overwrite(db_context: DBContext):
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA1
    )
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA2
    )

    res = with_db_context(query_all_elements, "SELECT * FROM censusdata.nuts;")
    assert len(res) == 2
    assert res[0] == (0, "DE", "Deutschland", 0)


def test_truncate_insert_dataframe_to_postgres_does_replace_existing_data(
    db_context: DBContext,
):
    connect_to_db_and_insert_pandas_dataframe(
        schema="coronacases", table="german_counties_more_info", data=DATA4
    )
    connect_to_db_and_truncate_insert_pandas_dataframe(
        schema="coronacases", table="german_counties_more_info", data=DATA4
    )

    res = with_db_context(
        query_all_elements, "SELECT * FROM coronacases.german_counties_more_info;"
    )
    print(res)
    assert len(res) == 2
    assert res[0] == (
        1,
        "Berlin",
        1,
        "Berlin",
        "A15-100",
        "M",
        datetime(2021, 10, 28, 0, 0),
        datetime(2021, 10, 28, 0, 0),
        True,
        0,
        -9,
        0,
        1,
        0,
        1,
    )


def test_generate_upsert_action_fragments(db_context: DBContext):
    upsert_action = _upsert_column_action(["blub"])
    assert (
        upsert_action.as_string(db_context["connection"]) == '"blub" = EXCLUDED."blub"'
    )


def test_generate_upsert_query(db_context: DBContext):
    df = pd.DataFrame(columns=["col1", "col2"], data=[[1, 2]])
    query = _build_upsert_query(
        "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {};",
        "bli",
        "bla",
        ["const"],
    )(df)
    assert (
        query.as_string(db_context["connection"])
        == 'INSERT INTO "bli"."bla" ("col1","col2") VALUES %s ON CONFLICT ("const") DO UPDATE SET '
        + '"col1" = EXCLUDED."col1", "col2" = EXCLUDED."col2";'
    )


def test_upsert_dataframe_to_postgres_does_overwrite(db_context: DBContext):
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA1
    )
    connect_to_db_and_upsert_pandas_dataframe(
        schema="censusdata", table="nuts", constraint=["geo"], data=DATA2
    )
    res = with_db_context(query_all_elements, "SELECT * FROM censusdata.nuts;")
    assert len(res) == 2
    assert res[0] == (1, "DE", "Deutschland", 1)


def test_query_builder_returns_correct_query(db_context):
    df = pd.DataFrame(columns=["col1", "col2"], data=[[1, 2]])

    query = _build_insert_query("schemaname", "tablename")(df)

    query_string = query.as_string(db_context["connection"])

    print(query_string)
    assert """"schemaname"."tablename" ("col1","col2")""" in query_string


DATA1 = pd.DataFrame(
    columns=["level", "geo", "name", "country_id"],
    data=[[0, "DE", "Deutschland", 0], [1, "DE1", "Berlin", 0]],
)

DATA2 = pd.DataFrame(
    columns=["level", "geo", "name", "country_id"],
    data=[[1, "DE", "Deutschland", 1], [1, "DE1", "Berlin", 1]],
)

DATA3 = po.DataFrame(
    columns=["date_of_report", "state", "incidence_7d_per_100k", "location_id"],
    data=[
        [0, 1, float("nan"), 0],
        [1, None, 1.0, 1],
    ],
)

DATA4 = pd.DataFrame(
    columns=[
        "stateid",
        "state",
        "countyid",
        "county",
        "agegroup",
        "sex",
        "date_cet",
        "ref_date_cet",
        "ref_date_is_symptom_onset",
        "is_new_case",
        "is_new_death",
        "is_new_recovered",
        "new_cases",
        "new_deaths",
        "new_recovereds",
    ],
    data=[
        [
            1,
            "Berlin",
            1,
            "Berlin",
            "A15-100",
            "M",
            "2021-10-28",
            "2021-10-28",
            True,
            0,
            -9,
            0,
            1,
            0,
            1,
        ],
        [
            2,
            "Berlin",
            2,
            "Berlin",
            "A15-100",
            "M",
            "2021-10-28",
            "2021-10-28",
            True,
            0,
            -9,
            0,
            1,
            0,
            1,
        ],
    ],
)
