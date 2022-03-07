import pandas as pd
from database import DBContext
from .write_dataframe_to_postgres import connect_to_db_and_insert_pandas_dataframe
from .load_dataframe_from_postgres import execute_query_and_return_dataframe


def test_load_dataframe_from_postgres(db_context: DBContext):
    connect_to_db_and_insert_pandas_dataframe(
        schema="censusdata", table="nuts", data=DATA1
    )
    res_from_db = execute_query_and_return_dataframe(
        """ SELECT * FROM censusdata.nuts;""", db_context
    )

    assert (res_from_db.values == DATA1.values).all()


DATA1 = pd.DataFrame(
    columns=["level", "geo", "name", "country_id"],
    data=[[0, "DE", "Deutschland", 0], [1, "DE1", "Berlin", 0]],
)
