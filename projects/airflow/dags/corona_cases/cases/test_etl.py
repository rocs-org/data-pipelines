import numpy
from psycopg2 import sql
from .etl import (
    etl_covid_cases,
    transform_dataframe,
    COLUMNS,
    TABLE,
    SCHEMA,
)
import datetime
from collections import Counter
import ramda as R
from database import (
    DBContext,
    execute_sql,
    create_db_context,
    query_all_elements,
    db_context,
)
from dags.helpers.dag_helpers import download_csv


URL = "http://static-files/static/coronacases.csv"


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


def test_dataframe_transformer_transform_column_names_and_types():
    df = R.pipe(download_csv, transform_dataframe)(URL)
    assert Counter(df.columns) == Counter(COLUMNS)

    assert df.iloc[0]["agegroup2"] is None
    assert df.iloc[1]["agegroup2"] is not None

    assert type(df.iloc[0]["ref_date_is_symptom_onset"]) is numpy.bool_


def test_download_csv_and_write_to_postgres_happy_path(db_context):

    etl_covid_cases(URL, SCHEMA, TABLE)

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
