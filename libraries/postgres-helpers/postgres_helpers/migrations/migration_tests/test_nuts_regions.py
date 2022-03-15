from postgres_helpers import DBContext, execute_sql, query_all_elements
from psycopg2 import sql

SCHEMA = "censusdata"
TABLE = "nuts"


def test_nuts_regions_schema(db_context: DBContext):
    columns = sql.SQL(",").join(
        sql.Identifier(name) for name in ["level", "geo", "name"]
    )
    query = sql.SQL(
        "INSERT INTO {schema}.{table} ({columns}) VALUES(%s, %s, %s);"
    ).format(
        schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE), columns=columns
    )
    execute_sql(db_context, query, (3, "DE121", "Irgendwo"))

    res = query_all_elements(db_context, f"SELECT * FROM {SCHEMA}.{TABLE}")
    print(res)
    assert len(res) == 1
