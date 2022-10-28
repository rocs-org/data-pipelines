from clickhouse_helpers import DBContext, query_dataframe
from typing import List, Dict
import pandas as pd
import ramda as R


@R.curry
def extract_static_data(
    context: DBContext, tables: List[str]
) -> Dict[str, pd.DataFrame]:
    static_data = {}
    for table in tables:
        static_data[table] = query_dataframe(context, f"SELECT * FROM {table}")
    return static_data


@R.curry
def load_static_data(context: DBContext, static_data: Dict[str, pd.DataFrame]) -> None:
    for table, df in static_data.items():
        columns = ", ".join(df.columns)
        context["connection"].execute(
            f"INSERT INTO {table}, ({columns}) VALUES",
            params=list(df.values.T),
            columnar=True,
            types_check=False,
        )
