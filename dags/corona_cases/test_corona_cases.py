import pandas as pd
import pytest
import os
import psycopg2
from .corona_cases import (
    download_csv_and_upload_to_postgres,
    write_dataframe_to_postgres,
    _build_query,
)
from dags.database.db_context import (
    create_db_context,
)
from ..database import DBContext
from ..database.execute_sql import query_all_elements

from dags.helpers.test_helpers import with_downloadable_csv

URL = "http://some.random.url/file.csv"
csv_content = """ObjectId,IdBundesland,Bundesland,Landkreis,Altersgruppe,Geschlecht,AnzahlFall,AnzahlTodesfall,Meldedatum,IdLandkreis,Datenstand,NeuerFall,NeuerTodesfall,Refdatum,NeuGenesen,AnzahlGenesen,IstErkrankungsbeginn,Altersgruppe2
1,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/10/30 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/27 00:00:00+00,0,1,1,Nicht übermittelt
2,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/10/30 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/29 00:00:00+00,0,1,1,Nicht übermittelt
3,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,3,0,2020/10/31 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/31 00:00:00+00,0,3,0,Nicht übermittelt
4,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/02 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/24 00:00:00+00,0,1,1,Nicht übermittelt
5,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/03 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/28 00:00:00+00,0,1,1,Nicht übermittelt
6,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/05 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/05 00:00:00+00,0,1,0,Nicht übermittelt
7,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/08 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/08 00:00:00+00,0,1,0,Nicht übermittelt
8,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/13 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/08 00:00:00+00,0,1,1,Nicht übermittelt
9,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/13 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/13 00:00:00+00,0,1,0,Nicht übermittelt
"""


columns = (
    "stateid",
    "state",
    "countyid",
    "county",
    "agegroup",
    "agegroup2",
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
)


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres_happy_path(db_context):
    table = "test_table"

    download_csv_and_upload_to_postgres(URL, table)

    context = create_db_context()
    results = query_all_elements(context, f"SELECT %s FROM %s" % (columns, table))

    assert len(results) == 10
    assert results[0] == ()


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres_picks_up_injected_db_name(db_context):
    table = "test_table"

    with pytest.raises(psycopg2.OperationalError) as exception_info:
        download_csv_and_upload_to_postgres(
            URL, table, dag_run={"conf": {"TARGET_DB": "rando_name"}}
        )

    assert 'database "rando_name" does not exist' in str(exception_info.value)
    assert os.environ["TARGET_DB"] == "rando_name"


def test_build_query():
    df = pd.DataFrame(
        columns=["ColOne", "col2", "col_3", "Col_4", "Col Five"], data=[[1, 2, 3, 4, 5]]
    )
    query = _build_query("test_table")(df)
    print(query)
    assert False


@pytest.mark.usefixtures("db_context")
def test_write_df_to_postgres(db_context: DBContext):
    df = pd.DataFrame(
        columns=["col1", "col2", "col3"],
        data=[[1, "hello", "world"], [2, "not", "today"]],
    )

    table = "test_table"

    res = write_dataframe_to_postgres(db_context, table)(df)
    print(res)

    assert query_all_elements(db_context, f"SELECT col1, col2, col3 FROM {table}") == [
        (1, "hello", "world"),
        (2, "not", "today"),
    ]
