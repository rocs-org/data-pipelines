import pandas as pd
import numpy
from psycopg2 import sql
from .corona_cases import (
    COLUMNS,
    TABLE,
    SCHEMA,
)
from .download_corona_cases import (
    download_csv_and_upload_to_postgres,
    transform_dataframe,
    _build_query,
)
import datetime
from collections import Counter
import ramda as R
from dags.database import DBContext, execute_sql, create_db_context, query_all_elements
from dags.helpers.test_helpers import with_downloadable_csv
from dags.helpers.dag_helpers import download_csv


URL = "http://some.random.url/file.csv"
csv_content = """ObjectId,IdBundesland,Bundesland,Landkreis,Altersgruppe,Geschlecht,AnzahlFall,AnzahlTodesfall,Meldedatum,IdLandkreis,Datenstand,NeuerFall,NeuerTodesfall,Refdatum,NeuGenesen,AnzahlGenesen,IstErkrankungsbeginn,Altersgruppe2
1,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/10/30 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/27 00:00:00+00,0,1,1,Nicht übermittelt
2,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/10/30 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/29 00:00:00+00,0,1,1,A35-A59
3,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,3,0,2020/10/31 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/31 00:00:00+00,0,3,0,Nicht übermittelt
4,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/02 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/24 00:00:00+00,0,1,1,Nicht übermittelt
5,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/03 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/10/28 00:00:00+00,0,1,1,Nicht übermittelt
6,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/05 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/05 00:00:00+00,0,1,0,Nicht übermittelt
7,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/08 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/08 00:00:00+00,0,1,0,Nicht übermittelt
8,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/13 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/08 00:00:00+00,0,1,1,Nicht übermittelt
9,1,Schleswig-Holstein,SK Flensburg,A35-A59,M,1,0,2020/11/13 00:00:00+00,01001,"02.08.2021, 00:00 Uhr",0,-9,2020/11/13 00:00:00+00,0,1,0,Nicht übermittelt
"""


def test_corona_cases_table_is_accessible(db_context: DBContext):
    execute_sql(
        db_context,
        """INSERT INTO coronacases.german_counties_more_info (
            stateid, 
            state, 
            countyid,
            county, 
            agegroup,
            date_cet,
            ref_date_cet,
            ref_date_is_symptom_onset
        ) VALUES (
            1, 
            1, 
            1, 
            'Berlin', 
            1, 
            to_timestamp('20/8/2013 14:52:49', 'DD/MM/YYYY hh24:mi:ss')::timestamp, 
            to_timestamp('20/8/2013 14:52:49', 'DD/MM/YYYY hh24:mi:ss')::timestamp, 
            true
        )""",
    )
    res = query_all_elements(db_context, f"SELECT * from {SCHEMA}.{TABLE}")
    assert len(res) == 1


@with_downloadable_csv(url=URL, content=csv_content)
def test_dataframe_transformer_transform_column_names():
    df = R.pipe(download_csv, transform_dataframe)(URL)
    assert Counter(df.columns) == Counter(COLUMNS)

    assert df.iloc[0]["agegroup2"] is None
    assert df.iloc[1]["agegroup2"] is not None

    assert type(df.iloc[0]["ref_date_is_symptom_onset"]) is numpy.bool_


def test_query_builder_returns_correct_query(db_context):
    df = pd.DataFrame(columns=["col1", "col2"], data=[[1, 2]])

    query = _build_query("schemaname", "tablename")(df)

    query_string = query.as_string(db_context["connection"])

    print(query_string)
    assert """"schemaname"."tablename" ("col1","col2")""" in query_string


@with_downloadable_csv(url=URL, content=csv_content)
def test_download_csv_and_write_to_postgres_happy_path(db_context):

    download_csv_and_upload_to_postgres(URL, SCHEMA, TABLE)

    context = create_db_context()

    print(R.path(["credentials", "database"], context))

    results = query_all_elements(
        context,
        sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(SCHEMA), sql.Identifier(TABLE)
        ),
    )
    print(results[0])
    assert len(results) == 9
    assert results[0] == (
        1,
        1,
        "Schleswig-Holstein",
        1001,
        "SK Flensburg",
        "A35-A59",
        None,
        "M",
        datetime.datetime(2020, 10, 30, 0, 0),
        datetime.datetime(2020, 10, 27, 0, 0),
        True,
        0,
        -9,
        0,
        1,
        0,
        1,
    )
