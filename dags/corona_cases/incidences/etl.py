import ramda as R
from .incidences import calculate_incidence, load_cases_data, load_counties_info
from dags.database import create_db_context, teardown_db_context, execute_sql
from dags.helpers.dag_helpers import connect_to_db_and_insert_polars_dataframe
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present


SCHEMA = "coronacases"
TABLE = "german_counties_incidence"
INCIDENCES_ARGS = [SCHEMA, TABLE]


def calculate_incidence_post_processing(schema, table, **kwargs):
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        clean_incidence_database(schema, table),
        R.converge(calculate_incidence, [load_counties_info, load_cases_data]),
        R.tap(lambda df: print(df.head())),
        connect_to_db_and_insert_polars_dataframe(schema, table),
        R.path(["credentials", "database"]),
    )(kwargs)


@R.curry
def clean_incidence_database(schema: str, table: str, args):
    context = create_db_context()
    execute_sql(context, f"TRUNCATE {schema}.{table};")
    teardown_db_context(context)
    return args
