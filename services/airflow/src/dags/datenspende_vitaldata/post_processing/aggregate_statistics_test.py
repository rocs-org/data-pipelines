import numpy as np
import pandas as pd

from postgres_helpers import DBContext
from src.lib.dag_helpers import execute_query_and_return_dataframe
from .aggregate_statistics import (
    pipeline_for_aggregate_statistics_of_per_user_vitals,
    calculate_aggregated_statistics_of_user_vitals,
)
from .shared_test import FIRST_TEST_USER_DATA, SECOND_TEST_USER_DATA
from .test_pivot_tables import setup_vitaldata_in_db


def test_integration_aggregate_statistics_pipeline(pg_context: DBContext):
    setup_vitaldata_in_db()

    pipeline_for_aggregate_statistics_of_per_user_vitals()
    aggregate_statistics = execute_query_and_return_dataframe(
        "SELECT * FROM datenspende_derivatives.daily_vital_statistics;", pg_context
    )
    assert (
        aggregate_statistics.query("user_id == 100 and type == 9 and source == 6")[
            "std"
        ].values[0]
        == 0
    )
    assert (
        aggregate_statistics.query("user_id == 100 and type == 9 and source == 6")[
            "mean"
        ].values[0]
        == pd.DataFrame(SECOND_TEST_USER_DATA)
        .query("user_id == 100 and type == 9 and source == 6")["value"]
        .mean()
    )


def test_calculate_aggregated_statistics_returns_nan_std_if_only_one_day_of_data_is_present():
    assert np.isnan(
        calculate_aggregated_statistics_of_user_vitals(
            pd.DataFrame(FIRST_TEST_USER_DATA)
        )["std"].values
    ).all()


def test_calculate_aggregated_statistics_returns_identity_for_mean_if_only_one_day_of_data_is_present():
    assert (
        calculate_aggregated_statistics_of_user_vitals(
            pd.DataFrame(FIRST_TEST_USER_DATA).sort_values(
                ["user_id", "type", "source"]
            )
        )["mean"].values
        == pd.DataFrame(FIRST_TEST_USER_DATA)
        .sort_values(["user_id", "type", "source"])["value"]
        .values
    ).all()


def test_calculate_aggregated_statistics_returns_mean_and_std_for_more_than_one_day_of_data():
    data = calculate_aggregated_statistics_of_user_vitals(
        pd.DataFrame(SECOND_TEST_USER_DATA)
    )
    assert (
        data.query("user_id == 100 and type == 9 and source == 6")["std"].values[0] == 0
    )
    assert (
        data.query("user_id == 100 and type == 9 and source == 6")["mean"].values[0]
        == pd.DataFrame(SECOND_TEST_USER_DATA)
        .query("user_id == 100 and type == 9 and source == 6")["value"]
        .mean()
    )


FIRST_TEST_USER_RESULT = {
    "user_id": {0: 200, 1: 200, 2: 200, 3: 200, 4: 200},
    "type": {0: 9, 1: 43, 2: 52, 3: 53, 4: 65},
    "source": {0: 6, 1: 6, 2: 6, 3: 6, 4: 6},
    "mean": {0: 3600.0, 1: 400.0, 2: 1634854000.0, 3: 1634879000.0, 4: 70.0},
    "std": {
        0: float("nan"),
        1: float("nan"),
        2: float("nan"),
        3: float("nan"),
        4: float("nan"),
    },
}
SECOND_TEST_USER_RESULT = {
    "user_id": {0: 100, 1: 100, 2: 100, 3: 100, 4: 100},
    "type": {0: 9, 1: 43, 2: 52, 3: 53, 4: 65},
    "source": {0: 6, 1: 3, 2: 3, 3: 3, 4: 3},
    "mean": {0: 4600.0, 1: 500.0, 2: 1634936000.0, 3: 1634965000.0, 4: 50.0},
    "std": {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0},
}
