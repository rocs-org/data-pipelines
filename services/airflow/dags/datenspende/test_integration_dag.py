from database import DBContext, query_all_elements
import pandas
import ramda as R
from airflow.models import DagBag

from dags.helpers.test_helpers import execute_dag


def test_dag_loads_with_no_errors():
    dag_bag = DagBag(include_examples=False)
    dag_bag.process_file("dag.py")
    assert len(dag_bag.import_errors) == 0


def test_datenspende_dag_writes_correct_results_to_db(db_context: DBContext):
    credentials = db_context["credentials"]

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
        11595,
        1134512,
        1544,
        5,
        2,
        31,
        0,
        1634199437933,
        130,
        "",
    )
    assert len(answers_from_db) == 5765

    single_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers;",
        R.prop("connection", db_context),
    )

    assert len(single_answers) == 575

    duplicated_answers = pandas.read_sql_query(
        "SELECT * FROM datenspende_derivatives.test_and_symptoms_answers_duplicates;",
        R.prop("connection", db_context),
    )

    assert len(duplicated_answers) == 29


THRYVE_FTP_URL = "http://static-files/thryve/exportStudy.7z"
POPULATION_URL = "http://static-files/static/demo_r_pjangrp3.tsv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/04-kreise.xlsx"
ZIP_URL = "http://static-files/static/pc2020_DE_NUTS-2021_v3.0.zip"
