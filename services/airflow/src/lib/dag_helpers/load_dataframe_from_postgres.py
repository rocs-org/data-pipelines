from typing import Union

import pandas as pd
import ramda as R
from psycopg2.sql import SQL, Composed

from postgres_helpers import DBContext


@R.curry
def execute_query_and_return_dataframe(
    query: Union[str, SQL, Composed], context: DBContext
):
    return pd.read_sql(query, con=context["connection"])
