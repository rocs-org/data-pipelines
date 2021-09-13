import ramda as R
from pandas import DataFrame
from returns.curry import curry

from dags.database import (
    DBContext,
    teardown_db_context,
)
from dags.helpers.dag_helpers import download_csv
from dags.helpers.dag_helpers import connect_to_db_and_insert
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present


@curry
def download_csv_and_upload_to_postgres(
    url: str, schema: str, table: str, **kwargs
) -> DBContext:
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        lambda *args: download_csv(url),
        transform_dataframe,
        connect_to_db_and_insert(schema, table),
        teardown_db_context,
        R.path(["credentials", "database"]),
    )(kwargs)


def transform_dataframe(df: DataFrame) -> DataFrame:
    print(df.columns)
    renamed = df.rename(columns=COLUMN_MAPPING, inplace=False).drop(
        columns=["Datenstand"]
    )
    renamed["agegroup2"] = renamed["agegroup2"].map(
        lambda x: x if not (R.is_nil(x) or x == "Nicht übermittelt") else None
    )
    renamed["ref_date_is_symptom_onset"] = renamed["ref_date_is_symptom_onset"].astype(
        bool
    )

    return renamed


def _raise(ex: Exception):
    raise ex


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
