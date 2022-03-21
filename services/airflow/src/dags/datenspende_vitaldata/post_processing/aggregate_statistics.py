import pandas as pd
from src.dags.datenspende_vitaldata.post_processing.shared import (
    post_processing_vitals_pipeline_factory,
)

DB_PARAMETERS = [
    "datenspende_derivatives",
    "daily_vital_statistics",
    ["one_value_per_user_and_type"],
]


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
