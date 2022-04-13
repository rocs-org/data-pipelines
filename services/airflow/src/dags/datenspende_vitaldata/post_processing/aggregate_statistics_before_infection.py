from psycopg2.sql import SQL, Identifier
from postgres_helpers import execute_sql
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present

DB_PARAMETERS = [
    "datenspende_derivatives",
    "daily_vital_statistics",
]


def aggregate_statistics_before_infection(schema: str, table: str, **kwargs) -> None:

    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    execute_sql(
        SQL(
            """
            REFRESH MATERIALIZED VIEW {schema}.{table};
        """
        ).format(schema=Identifier(schema), table=Identifier(table))
    )
