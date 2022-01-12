from typing import List, Tuple

import pandas as pd
import ramda as R

from src.lib.dag_helpers import (
    connect_to_db_and_upsert_pandas_dataframe_on_constraint,
)


@R.curry
def upload_all_dataframes(schema: str, dataframes: List[Tuple[str, str, pd.DataFrame]]):
    return R.map(
        lambda item: connect_to_db_and_upsert_pandas_dataframe_on_constraint(
            schema, *item
        ),
        dataframes,
    )
