from postgres_helpers import DBContext
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    load_dbt_nodes_from_file,
)
from src.lib.test_helpers import run_task_with_url, run_task
import pytest
import networkx as nx


def test_standardized_vitals_pipeline(db: DBContext):

    # user 1213338 does like a million steps a day. We don't want them in our data.
    # Make sure, steps aggregates are reasonable i.e. average of steps per day is below 10.000
    aggregates_by_source = execute_query_and_return_dataframe(
        """
        SELECT
            mean
        FROM
            datenspende_derivatives.daily_aggregates_of_vitals
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
    assert all([val == pytest.approx(0) for val in stats["mean"].values])
    assert all([val == pytest.approx(1) for val in stats["std"].values])


def test_standardized_vitals_pipeline_excludes_zero_std(db: DBContext):
    # it may be, that users have constant vital data (for whatever reason)
    # this means zero standard deviation which breaks standardization. We have to exclude those.
    vitals_standardized_by_day = execute_query_and_return_dataframe(
        """
        SELECT
            user_id, std_from_standardized, std_from_subtracted_mean
        FROM
            datenspende_derivatives.agg_before_infection_from_vitals_std_by_day
        WHERE
            user_id IN (326087, 372529);
        """,
        db,
    )
    # assert standard deviations are zero
    assert (
        vitals_standardized_by_day[
            ["std_from_standardized", "std_from_subtracted_mean"]
        ].values
        == [[0, 0], [0, 0]]
    ).all()

    # assert standardized values table is populated nonetheless
    assert (
        len(
            execute_query_and_return_dataframe(
                """
        SELECT
            *
        FROM
            datenspende_derivatives.vitals_std_by_date_and_user_before_infection
        """,
                db,
            ).values
        )
        > 0
    )

    # however, not with values for users where std is zero
    assert (
        len(
            execute_query_and_return_dataframe(
                """
        SELECT
            *
        FROM
            datenspende_derivatives.vitals_std_by_date_and_user_before_infection
        WHERE
            user_id IN (326087, 372529);
        """,
                db,
            ).values
        )
        == 0
    )


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
        run_task(
            "datenspende_surveys_v2",
            "extract_features_from_weekly_tasks",
        )
        run_task(
            "datenspende_surveys_v2",
            "extract_features_from_one_off_answers",
        )

        # Run dbt tasks in the order of their dependencies
        node_data = load_dbt_nodes_from_file("/opt/airflow/dbt/target/manifest.json")

        # sort nodes by dependencies
        g = nx.DiGraph()
        for node in list(node_data.keys()):
            dependencies = node_data[node]["depends_on"]["nodes"]
            for dep in dependencies:
                g.add_edge(dep, node)
        sorted_nodes = list(nx.topological_sort(g))

        # create list of task names (exclude source models)
        sorted_tasks = [
            "dbt_run_" + x for x in sorted_nodes if x.split(".")[0] == "model"
        ]

        # run dbt tasks
        for task in sorted_tasks:
            run_task(
                "datenspende_vitaldata_v2",
                task,
            )

        yield pg_context
    finally:
        pass
