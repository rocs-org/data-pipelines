import ramda as R
import pandas as pd
from pandas import DataFrame
from returns.curry import curry

from postgres_helpers import DBContext
from src.lib.dag_helpers import connect_to_db_and_truncate_insert_pandas_dataframe
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present


URL = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/blob/main/Aktuell_Deutschland_SarsCov2_Infektionen.csv?raw=true"  # noqa: E501
SCHEMA = "coronacases"
TABLE = "german_counties_more_info"

CASES_ARGS = [URL, SCHEMA, TABLE]


@curry
def etl_covid_cases(url: str, schema: str, table: str, **kwargs) -> DBContext:
    print("load cases from ", url)
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: pd.read_csv(url),
        transform_dataframe,
        connect_to_db_and_truncate_insert_pandas_dataframe(schema, table),
        R.path(["credentials", "database"]),
    )(kwargs)


def transform_dataframe(df: DataFrame) -> DataFrame:
    additional_info = pd.read_csv("http://static-files/static/countyID_mapping.csv")
    df = df.join(additional_info.set_index("IdLandkreis"), on="IdLandkreis")
    renamed = df.rename(columns=COLUMN_MAPPING, inplace=False)
    renamed["date_cet"] = pd.to_datetime(renamed["date_cet"])
    renamed["ref_date_cet"] = pd.to_datetime(renamed["ref_date_cet"])
    renamed["ref_date_is_symptom_onset"] = renamed["ref_date_is_symptom_onset"].astype(
        bool
    )

    return renamed


COLUMN_MAPPING = {
    "IdBundesland": "stateid",
    "Bundesland": "state",
    "Landkreis": "county",
    "IdLandkreis": "countyid",
    "Altersgruppe": "agegroup",
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
