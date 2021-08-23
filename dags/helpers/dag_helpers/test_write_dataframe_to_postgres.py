import pandas as pd
from .write_dataframe_to_postgres import _build_insert_query, connect_to_db_and_insert
from dags.database import DBContext, query_all_elements


def test_insert_dataframe_to_postgres_writes_data_correctly(db_context: DBContext):

    connect_to_db_and_insert(schema="censusdata", table="nuts", data=DATA1)

    res = query_all_elements(db_context, "SELECT * FROM censusdata.nuts;")
    assert len(res) == 2
    assert res[0] == (0, "DE", "Deutschland", 0)


def test_insert_dataframe_to_postgres_does_not_overwrite(db_context: DBContext):
    connect_to_db_and_insert(schema="censusdata", table="nuts", data=DATA1)
    connect_to_db_and_insert(schema="censusdata", table="nuts", data=DATA2)

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
