from .etl import etl_german_counties_more_info, SCHEMA, TABLE
from postgres_helpers import (
    DBContext,
    query_all_elements,
)
from psycopg2 import sql
import datetime

from src.dags.nuts_regions_population.nuts_regions import (
    etl_eu_regions,
    REGIONS_ARGS,
)

URL = "http://static-files/static/04-kreise.xlsx"


def test_etl_writes_data_to_database(pg_context: DBContext):

    etl_eu_regions(
        "http://static-files/static/NUTS2021.xlsx", REGIONS_ARGS[1], REGIONS_ARGS[2]
    )

    etl_german_counties_more_info(URL, SCHEMA, TABLE)

    data_from_db = query_all_elements(
        pg_context,
        sql.SQL("SELECT * FROM {schema}.{table}").format(
            schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE)
        ),
    )
    assert len(data_from_db) == 15
    assert data_from_db[0] == FIRST_ROW


FIRST_ROW = (
    1,
    1001,
    "Kreisfreie Stadt",
    "Flensburg, Stadt",
    "DEF01",
    53.02,
    90164,
    44904,
    45260,
    1701.0,
    datetime.date(2019, 12, 31),
)
