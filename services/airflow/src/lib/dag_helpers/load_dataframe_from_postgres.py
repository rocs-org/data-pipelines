import ramda as R
import pandas as pd
from database import DBContext


@R.curry
def execute_query_and_return_dataframe(query: str, context: DBContext):
    return pd.read_sql(query, con=context["connection"])
