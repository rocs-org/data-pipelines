from psycopg2.sql import SQL, Identifier

from postgres_helpers import create_db_context, execute_sql, teardown_db_context
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present


def refresh_materialized_view(schema: str, table: str, **kwargs) -> None:

    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    db_context = create_db_context()
    execute_sql(
        db_context,
        SQL(
            """
            REFRESH MATERIALIZED VIEW {schema}.{table};
        """
        ).format(schema=Identifier(schema), table=Identifier(table)),
    )
    teardown_db_context(db_context)
