from typing import List, Tuple

import pandas as pd
import ramda as R

from dags.helpers.dag_helpers import connect_to_db_and_insert_pandas_dataframe


@R.curry
def upload_all_dataframes(schema: str, dataframes: List[Tuple[str, pd.DataFrame]]):
    return R.map(
        lambda item: connect_to_db_and_insert_pandas_dataframe(
            schema, item[0], item[1]
        ),
        dataframes,
    )
