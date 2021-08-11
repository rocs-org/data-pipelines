from .upload_to_postgres import upload_to_postgres
from .download_nuts_regions import download_nuts_regions
from .transform_nuts_regions import transform_nuts_regions
from .dag import SCHEMA, TABLE
from dags.database import DBContext, query_all_elements
from psycopg2 import sql
import ramda as R

URL = "http://static-files/static/NUTS2021.xlsx"


def test_upload_to_postgres_writes_regions_to_database(db_context: DBContext):
    res = R.pipe(
        download_nuts_regions,
        transform_nuts_regions,
        upload_to_postgres(SCHEMA, TABLE),
    )(URL)

    print(res)

    query = sql.SQL("SELECT * FROM {schema}.{table};").format(
        schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE)
    )

    results_from_db = query_all_elements(db_context, query)

    assert len(results_from_db) == 2121
