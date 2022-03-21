import pandas as pd
import ramda as R
from typing import Callable
from postgres_helpers import create_db_context, teardown_db_context
from src.dags.datenspende_vitaldata.post_processing.shared import load_per_user_data
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from src.lib.dag_helpers import (
    upsert_pandas_dataframe_to_table_in_schema_with_db_context,
)

DB_PARAMETERS = [
    "datenspende_derivatives",
    "daily_vital_statistics",
    ["one_value_per_user_and_type"],
]


@R.curry
def post_processing_vitals_pipeline_factory(
    aggregator: Callable[[pd.DataFrame], pd.DataFrame], db_parameters
):
    def pipeline(**kwargs) -> None:
        set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
        db_context = create_db_context()
        R.map(
            R.pipe(
                aggregator,
                upsert_pandas_dataframe_to_table_in_schema_with_db_context(
                    db_context, *db_parameters
                ),
            ),
            load_per_user_data(db_context),
        )
        teardown_db_context(db_context)

    return pipeline


def calculate_aggregated_statistics_of_user_vitals(
    user_vital_data: pd.DataFrame,
) -> pd.DataFrame:
    return (
        user_vital_data.drop(columns=["date"])
        .groupby(["user_id", "type", "source"])
        .agg({"value": ["mean", "std"]})
        .droplevel(level=0, axis="columns")
        .reset_index()
    )


pipeline_for_aggregate_statistics_of_per_user_vitals = (
    post_processing_vitals_pipeline_factory(
        calculate_aggregated_statistics_of_user_vitals, DB_PARAMETERS
    )
)
