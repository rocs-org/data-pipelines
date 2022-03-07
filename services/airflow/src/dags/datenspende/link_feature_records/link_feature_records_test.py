import ramda as R
from database import DBContext
from src.lib.test_helpers import run_task_with_url, run_task
from src.lib.dag_helpers import execute_query_and_return_dataframe
from .link_feature_records import (
    link_subsequent_dates_for_same_user_id,
    write_links_to_db,
)


def test_write_links_to_db_only_updates_link_columns(db_context: DBContext):
    # fill database
    run_task_with_url(
        "datenspende_surveys_v2",
        "gather_data_from_thryve",
        "http://static-files/thryve/exportStudy.7z",
    )
    run_task(
        "datenspende_surveys_v2",
        "extract_features_from_weekly_tasks",
    )
    run_task(
        "datenspende_surveys_v2",
        "extract_features_from_one_off_answers",
    )

    R.pipe(link_subsequent_dates_for_same_user_id, write_links_to_db)("")

    res_from_db = execute_query_and_return_dataframe(
        """
        SELECT
            user_id, test_week_start, f10, id, next, previous
        FROM
            datenspende_derivatives.homogenized_features
        """,
        db_context,
    )

    # assert record 49 dates after 48
    assert (
        res_from_db["test_week_start"][res_from_db["id"] == 48].values[0]
        < res_from_db["test_week_start"][res_from_db["id"] == 49].values[0]
    )

    # assert entries are linked
    assert res_from_db[res_from_db["id"] == 49]["previous"].values[0] == 48
    assert res_from_db[res_from_db["id"] == 48]["next"].values[0] == 49

    # other features (e.g. test results) are not over writen
    assert not res_from_db[res_from_db["id"].isin([48, 49])]["f10"].isna().values.all()
