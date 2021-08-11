import pandas as pd
from .write_dataframe_to_postgres import _build_query


def test_query_builder_returns_correct_query(db_context):
    df = pd.DataFrame(columns=["col1", "col2"], data=[[1, 2]])

    query = _build_query("schemaname", "tablename")(df)

    query_string = query.as_string(db_context["connection"])

    print(query_string)
    assert """"schemaname"."tablename" ("col1","col2")""" in query_string
