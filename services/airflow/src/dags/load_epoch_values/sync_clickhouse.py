from datetime import date, timedelta
from typing import List, Dict

import pandas as pd
import ramda as R
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present

from clickhouse_helpers import DBContext, query_dataframe, create_db_context


def extract_load_epoch_data(**kwargs):

    external_table = R.path_or(
        "DynamicEpochValue", ["dag_run", "conf", "external_table"], kwargs
    )
    prefix = R.path_or("raw_", ["dag_run", "conf", "prefix"], kwargs)

    set_env_variable_from_dag_config_if_present("CLICKHOUSE_DB", kwargs)
    set_env_variable_from_dag_config_if_present("EXTERNAL_CLICKHOUSE_DB", kwargs)

    external_clickhouse_context = create_db_context(env_prefix="EXTERNAL_CLICKHOUSE")
    clickhouse_context = create_db_context(env_prefix="CLICKHOUSE")

    execution_date = kwargs["execution_date"]
    execution_date = date(execution_date.year, execution_date.month, execution_date.day)

    return R.pipe(
        extract_data_between(
            external_clickhouse_context,
            execution_date - timedelta(days=1),
            execution_date,
            "createdAt",
        ),
        append_prefix_to_dict_keys(prefix),
        load_data(clickhouse_context),
    )([external_table])


@R.curry
def append_prefix_to_dict_keys(prefix: str, d: Dict) -> Dict:
    return {prefix + k: v for k, v in d.items()}


@R.curry
def extract_data_between(
    context: DBContext,
    start_time: date,
    end_time: date,
    date_column_name: str,
    tables: List[str],
) -> Dict[str, pd.DataFrame]:
    static_data = {}
    for table in tables:
        query = f"SELECT * FROM {table} where {date_column_name} >= '{start_time}' and {date_column_name} <= '{end_time}'"
        print(query)
        static_data[table] = query_dataframe(
            context,
            query,
        )
    return static_data


@R.curry
def load_data(context: DBContext, static_data: Dict[str, pd.DataFrame]) -> None:
    for table, df in static_data.items():

        columns = ", ".join(df.columns)
        query = f"INSERT INTO {table} ({columns}) VALUES"

        try:
            context["connection"].execute(
                query,
                params=list(df.values.T),
                columnar=True,
                types_check=False,
            )
        except Exception as e:
            print(query)
            print(e)
            raise e
