import ramda as R
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from dags.corona_cases.download_corona_cases import download_csv_and_upload_to_postgres

URL = "https://drive.google.com/uc?export=download&id=1t_WFejY2lXj00Qkc-6RAFgyr4sm5woQz"


COLUMN_MAPPING = {
    "ObjectId": "caseid",
    "IdBundesland": "stateid",
    "Bundesland": "state",
    "IdLandkreis": "countyid",
    "Landkreis": "county",
    "Altersgruppe": "agegroup",
    "Altersgruppe2": "agegroup2",
    "Geschlecht": "sex",
    "Meldedatum": "date_cet",
    "Refdatum": "ref_date_cet",
    "IstErkrankungsbeginn": "ref_date_is_symptom_onset",
    "NeuerFall": "is_new_case",
    "NeuerTodesfall": "is_new_death",
    "NeuGenesen": "is_new_recovered",
    "AnzahlFall": "new_cases",
    "AnzahlTodesfall": "new_deaths",
    "AnzahlGenesen": "new_recovereds",
}


COLUMNS = R.pipe(lambda x: x.values(), list)(COLUMN_MAPPING)

SCHEMA = "coronacases"
TABLE = "german_counties_more_info"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry": False,
    "provide_context": True,
}

dag = DAG(
    "corona_cases",
    default_args=default_args,
    description="an example DAG that downloads a csv and uploads it to postgres",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
)

t1 = PythonOperator(
    task_id="extract",
    python_callable=download_csv_and_upload_to_postgres,
    dag=dag,
    op_args=[URL, SCHEMA, TABLE],
)
