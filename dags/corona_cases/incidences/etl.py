import ramda as R
from .incidences import calculate_incidence, load_cases_data, load_counties_info
from dags.helpers.dag_helpers import connect_to_db_and_insert
from dags.helpers.test_helpers import set_env_variable_from_dag_config_if_present
from dags.database import teardown_db_context

SCHEMA = "coronacases"
TABLE = "german_counties_incidence"
INCIDENCES_ARGS = [SCHEMA, TABLE]


def incidences_etl(schema, table, **kwargs):
    return R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        R.converge(calculate_incidence, [load_counties_info, load_cases_data]),
        R.tap(print),
        connect_to_db_and_insert(schema, table),
        R.path(["credentials", "database"]),
    )(kwargs)
