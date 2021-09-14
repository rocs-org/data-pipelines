from .german_counties_etl import german_counties_more_info_etl, SCHEMA, TABLE
from dags.database import DBContext, query_all_elements, create_db_context
from psycopg2 import sql
import datetime

from dags.nuts_regions_population.nuts_regions import regions_task, REGIONS_ARGS

URL = "http://static-files/static/04-kreise.xlsx"


def test_etl_writes_data_to_database(db_context: DBContext):

    regions_task(
        "http://static-files/static/NUTS2021.xlsx", REGIONS_ARGS[1], REGIONS_ARGS[2]
    )

    german_counties_more_info_etl(URL, SCHEMA, TABLE)

    db_context = create_db_context()
    data_from_db = query_all_elements(
        db_context,
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