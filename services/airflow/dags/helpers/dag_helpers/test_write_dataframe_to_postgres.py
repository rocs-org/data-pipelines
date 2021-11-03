import numpy as np
import pandas as pd
import polars as po
from datetime import date, timedelta, datetime
from .write_dataframe_to_postgres import (
    _build_insert_query,
    connect_to_db_and_insert_pandas_dataframe,
    connect_to_db_and_insert_polars_dataframe,
)
from database import (
    DBContext,
    query_all_elements,
    create_db_context,
)


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
    db_context = create_db_context()

    res = query_all_elements(
        db_context, "SELECT * FROM coronacases.german_counties_incidence;"
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
    db_context = create_db_context()

    res = query_all_elements(db_context, "SELECT * FROM censusdata.nuts;")
    assert len(res) == 2
    assert res[0] == (0, "DE", "Deutschland", 0)


def test_insert_dataframe_to_postgres_does_not_overwrite(db_context: DBContext):
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA1
    )
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA2
    )

    db_context = create_db_context()
    res = query_all_elements(db_context, "SELECT * FROM censusdata.nuts;")
    assert len(res) == 2
    assert res[0] == (0, "DE", "Deutschland", 0)


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
    nullable=True,
)
