import pandas as pd
from .execute_sql import (
    snake_case_to_camel_case,
    camel_case_to_snake_case,
    execute_sql,
    insert_dataframe,
    query_dataframe,
)

from . import DBContext


def execute_sql_writes_to_and_reads_from_db(db_context: DBContext):
    execute_sql(
        db_context,
        "INSERT INTO test_table (col1, col2, col3) VALUES",
        [(1, "Hello", "World!")],
    )
    assert (
        execute_sql(
            db_context,
            """
             SELECT col1, col2, col3 FROM test_table;
        """,
        )
        == [(1, "Hello", "World!")]
    )


def test_write_and_read_dataframe_returns_dataframe_with_correct_values(
    db_context: DBContext,
):
    insert_dataframe(db_context, "test_table", TEST_DF.set_index("id"))

    res = query_dataframe(db_context, "SELECT * FROM test_table")
    assert (res.values == TEST_DF.values).all()


def test_insert_dataframe_works_regardless_of_column_order(db_context: DBContext):
    insert_dataframe(
        db_context,
        "test_table",
        pd.DataFrame(
            {"col1": [3, 4], "col2": ["a", "b"], "id": [1, 2], "col3": ["c", "d"]}
        ).set_index("id"),
    )

    res = query_dataframe(db_context, "SELECT * FROM test_table")
    assert (res.values == TEST_DF.values).all()


def test_insert_dataframe_works_with_missing_columns(db_context: DBContext):
    insert_dataframe(
        db_context,
        "test_table",
        pd.DataFrame({"col2": ["a", "b"], "id": [1, 2]}).set_index("id"),
    )

    res = query_dataframe(db_context, "SELECT * FROM test_table")
    print(res.values)

    # Note, if value is not Nullable table definition,
    # missing strings are cast to empty string and missing ints are cast to 0

    assert (
        res.values
        == pd.DataFrame(
            {"id": [1, 2], "col1": [0, 0], "col2": ["a", "b"], "col3": ["", ""]}
        ).values
    ).all()


def test_snake_case_to_camel_case():
    assert snake_case_to_camel_case("snake_case") == "SnakeCase"


def test_camel_case_to_snake_case():
    assert camel_case_to_snake_case("CamelCase") == "camel_case"
    assert camel_case_to_snake_case("snake_case") == "snake_case"


TEST_DF = pd.DataFrame(
    {"id": [1, 2], "col1": [3, 4], "col2": ["a", "b"], "col3": ["c", "d"]}
)
