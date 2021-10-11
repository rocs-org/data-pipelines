from dags.database import DBContext
from dags.database.execute_sql import query_all_elements
from airflow.models import DagBag

from dags.helpers.test_helpers import execute_dag
from dags.corona_cases.cases import CASES_ARGS
from dags.corona_cases.incidences import INCIDENCES_ARGS


[_, CASES_SCHEMA, CASES_TABLE] = CASES_ARGS
[INCIDENCES_SCHEMA, INCIDENCES_TABLE] = INCIDENCES_ARGS


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_dag_writes_correct_results_to_db(db_context: DBContext):
    credentials = db_context["credentials"]

    # run population data pipeline first, as incidence calculation depends on its results
    #
    # assert (
    #     execute_dag(
    #         "nuts_regions_population",
    #         "2021-01-01",
    #         {
    #             "TARGET_DB": credentials["database"],
    #             "POPULATION_URL": POPULATION_URL,
    #             "REGIONS_URL": REGIONS_URL,
    #             "COUNTIES_URL": COUNTIES_URL,
    #             "ZIP_URL": ZIP_URL,
    #         },
    #     )
    # ) == 0

    assert (
        execute_dag(
            "datenspende",
            "2021-01-01",
            {"TARGET_DB": credentials["database"], "URL": THRYVE_FTP_URL},
        )
        == 0
    )
    answers_from_db = query_all_elements(
        db_context, "SELECT * FROM datenspende.answers"
    )
    assert answers_from_db[-1] == (
        7258,
        1156990,
        1003,
        1,
        1,
        4,
        0,
        1632844797833,
        13,
        "",
    )
    assert len(answers_from_db) == 5126


THRYVE_FTP_URL = "http://static-files/thryve/exportStudy.7z"
POPULATION_URL = "http://static-files/static/demo_r_pjangrp3.tsv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/04-kreise.xlsx"
ZIP_URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"
