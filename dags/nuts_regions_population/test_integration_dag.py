from airflow.models import DagBag
from dags.database import DBContext, query_all_elements
from dags.helpers.test_helpers import execute_dag
from psycopg2 import sql
from dags.nuts_regions_population.nuts_regions import REGIONS_ARGS
from dags.nuts_regions_population.population import POPULATION_ARGS
from dags.nuts_regions_population.german_counties_more_info import COUNTIES_ARGS

[_, REGIONS_SCHEMA, REGIONS_TABLE] = REGIONS_ARGS
[_, POPULATION_SCHEMA, POPULATION_TABLE] = POPULATION_ARGS
[_, COUNTIES_SCHEMA, COUNTIES_TABLE] = COUNTIES_ARGS

POPULATION_URL = "http://static-files/static/demo_r_pjangrp3.tsv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/04-kreise.xlsx"


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("nuts_regions_etl.py")
    assert len(dag_bag.import_errors) == 0


def test_dag_executes_and_writes_entries_to_DB(db_context: DBContext):
    credentials = db_context["credentials"]

    assert (
        execute_dag(
            "nuts_regions_population",
            "2021-01-01",
            {
                "TARGET_DB": credentials["database"],
                "POPULATION_URL": POPULATION_URL,
                "REGIONS_URL": REGIONS_URL,
                "COUNTIES_URL": COUNTIES_URL,
            },
        )
        == 0
    )

    regions_from_db = query_all_elements(
        db_context,
        sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(REGIONS_SCHEMA), sql.Identifier(REGIONS_TABLE)
        ),
    )
    assert len(regions_from_db) == 2121

    german_counties_from_db = query_all_elements(
        db_context,
        sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(COUNTIES_SCHEMA), sql.Identifier(COUNTIES_TABLE)
        ),
    )
    assert len(german_counties_from_db) == 15
