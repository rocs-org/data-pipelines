from postgres_helpers import DBContext, execute_sql
from src.lib.dag_helpers import execute_query_and_return_dataframe
from src.lib.test_helpers import run_task_with_url
import pytest


def test_standardized_vitals_pipeline(db: DBContext):

    # user 1213338 does like a million steps a day. We don't want them in our data.
    # Make sure, steps aggregates are reasonable i.e. average of steps per day is below 10.000
    aggregates_by_source = execute_query_and_return_dataframe(
        """
        SELECT
            mean
        FROM
            datenspende_derivatives.aggregates_for_standardization_by_type_source_date
        WHERE
            type = 9 AND
            mean > 10000;
        """,
        db,
    )
    assert len(aggregates_by_source["mean"].values) == 0

    # assert that standardization works as expected
    standardized_by_day = execute_query_and_return_dataframe(
        """
        SELECT
            *
        FROM
            datenspende_derivatives.vitals_standardized_by_daily_aggregates
        """,
        db,
    )

    stats = (
        standardized_by_day.groupby(["date", "source", "type"])
        .agg({"standardized_value": ["mean", "std"]})
        .dropna()
        .droplevel(level=0, axis="columns")
    )
    print(stats)
    assert (stats["mean"] == 0).values.all()
    assert (stats["std"] == 1).values.all()


@pytest.fixture
def db(pg_context):
    try:
        run_task_with_url(
            "datenspende_vitaldata_v2",
            "gather_vital_data_from_thryve",
            "http://static-files/thryve/export_with_outlier.7z",
        )
        run_task_with_url(
            "datenspende_surveys_v2",
            "gather_data_from_thryve",
            "http://static-files/thryve/exportStudy_reduced.7z",
        )
        execute_sql(
            pg_context,
            """
            REFRESH MATERIALIZED VIEW
                datenspende_derivatives.daily_vital_statistics_before_infection
            """,
        )
        execute_sql(
            pg_context,
            """
            REFRESH MATERIALIZED VIEW
                datenspende_derivatives.aggregates_for_standardization_by_type_source_date
            """,
        )
        execute_sql(
            pg_context,
            """
            REFRESH MATERIALIZED VIEW
                datenspende_derivatives.vitals_standardized_by_daily_aggregates
            """,
        )
        execute_sql(
            pg_context,
            """
            REFRESH MATERIALIZED VIEW
                datenspende_derivatives.vital_stats_before_infection_from_vitals_standardized_by_day
            """,
        )
        execute_sql(
            pg_context,
            """
            REFRESH MATERIALIZED VIEW
                datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection
            """,
        )
        yield pg_context
    finally:
        pass
