from typing import List
from psycopg2 import sql
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from database import (
    create_db_context,
    teardown_db_context,
    execute_sql,
    query_all_elements,
)


PIVOT_TARGETS = [[(9, "steps", "int"), (65, "resting_heartrate", "int")]]


def pivot_vitaldata(pivot_targets: List, **kwargs):
    set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
    db_context = create_db_context()
    for vitalid, vitaltype, datatype in pivot_targets:
        create_pivot_table(db_context, vitalid, vitaltype, datatype)
    teardown_db_context(db_context)


def create_pivot_table(db_context, vitalid, vitaltype, datatype):
    dates = query_all_elements(
        db_context,
        "SELECT DISTINCT to_char(date, 'YYYY-MM-DD') from datenspende.vitaldata ORDER BY 1;",
    )
    dates = [res[0] for res in dates]
    sql_query = sql.SQL(
        "DROP TABLE IF EXISTS datenspende_derivatives.{vitaltype};\
        CREATE TABLE datenspende_derivatives.{vitaltype} AS SELECT * FROM \
            crosstab('SELECT user_id, date, value FROM datenspende.vitaldata WHERE type = {vitalid} ORDER BY 1', \
            'SELECT DISTINCT date from datenspende.vitaldata ORDER BY 1') as columns(user_id int, {columns} int); \
            CREATE INDEX on datenspende_derivatives.{vitaltype} (user_id);"
    ).format(
        vitaltype=sql.Identifier(vitaltype + "_ct"),
        vitalid=sql.Literal(vitalid),
        columns=sql.SQL(" int, ").join([sql.Identifier(date) for date in dates]),
    )
    execute_sql(db_context, sql_query)
