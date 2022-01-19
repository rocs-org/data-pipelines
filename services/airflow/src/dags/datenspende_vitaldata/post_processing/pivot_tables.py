from database import create_db_context, teardown_db_context, execute_sql, query_all_elements
from psycopg2 import sql


PIVOT_TARGETS = [(9, 'steps', 'int'), (65, 'resting_heartrate', 'int')]

def pivot_vitaldata():
    db_context = create_db_context()
    for vitalid, vitaltype, datatype in PIVOT_TARGETS:
        create_pivot_table(db_context, vitalid, vitaltype, datatype)
    teardown_db_context(db_context)


def create_pivot_table(db_context, vitalid, vitaltype, datatype):
    dates = query_all_elements(db_context, "SELECT DISTINCT to_char(date, 'YYYY-MM-DD') from datenspende.vitaldata ORDER BY 1;")
    dates = [res[0] for res in dates]
    sql_query = "DROP TABLE IF EXISTS datenspende_derivatives.{vitaltype}_ct; CREATE TABLE datenspende_derivatives.{vitaltype}_ct AS SELECT * FROM crosstab('SELECT user_id, date, value FROM datenspende.vitaldata WHERE type = {vitalid} ORDER BY 1', 'SELECT DISTINCT date from datenspende.vitaldata ORDER BY 1') as columns(user_id int, {columns});".format(
        vitaltype=vitaltype,
        vitalid=vitalid,
        columns=', '.join(['"'+date+'" '+datatype for date in dates])
        )
    execute_sql(db_context, sql_query)
