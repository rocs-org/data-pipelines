import datetime
import os
from datetime import timedelta
from typing import Callable
from typing import List, TypedDict

import pandas as pd
import ramda as R
from pathos.multiprocessing import Pool
from psycopg2.sql import SQL, Literal

from postgres_helpers import (
    DBContext,
    create_db_context,
    teardown_db_context,
    query_one_element,
)
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    upsert_pandas_dataframe_to_table_in_schema_with_db_context,
)
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present


@R.curry
def post_processing_vitals_pipeline_factory(
    aggregator: Callable[[pd.DataFrame], pd.DataFrame],
    db_parameters,
    batch_size: int,
    load_last_n_days: int,
):
    def pipeline(**kwargs) -> None:
        # to inject the target db name and worker pool size into the worker env during testing
        set_env_variable_from_dag_config_if_present("TARGET_DB", kwargs)
        worker_pool_size = int(os.environ["WORKER_POOL_SIZE"])

        # load user id batches on main node
        db_context = create_db_context()
        user_id_batches = get_user_id_intervals(db_context, batch_size)
        teardown_db_context(db_context)

        # map over list of user id batches and process them on worker nodes
        with Pool(worker_pool_size) as pool:
            result = pool.map_async(
                extract_process_load_vital_data_for_user_batch(
                    aggregator,
                    kwargs["execution_date"] - timedelta(days=load_last_n_days),
                    db_parameters,
                ),
                user_id_batches,
            )

            result.wait()
            print("Done waiting for workers")

    return pipeline


class Interval(TypedDict):
    min: int
    max: int


@R.curry
def extract_process_load_vital_data_for_user_batch(
    aggregator, after: datetime.date, db_parameters, user_id_interval: Interval
) -> None:
    db_context = create_db_context()
    R.pipe(
        load_user_vitals_after_date(db_context, after),
        aggregator,
        upsert_pandas_dataframe_to_table_in_schema_with_db_context(
            db_context, *db_parameters
        ),
    )(user_id_interval)
    teardown_db_context(db_context)


def get_user_id_intervals(db_context: DBContext, interval_size: int) -> List[Interval]:
    min_id = load_min_user_id(db_context)
    max_id = load_max_user_id(db_context)
    intervals = []
    while min_id <= max_id:
        intervals += [{"min": min_id, "max": min(min_id + interval_size - 1, max_id)}]
        min_id += interval_size

    return intervals


def load_min_user_id(db_context: DBContext) -> int:
    return query_one_element(
        db_context, SQL("""SELECT min(user_id) FROM datenspende.vitaldata;""")
    )


def load_max_user_id(db_context: DBContext) -> int:
    return query_one_element(
        db_context, SQL("""SELECT max(user_id) FROM datenspende.vitaldata;""")
    )


@R.curry
def load_user_vitals_after_date(
    db_context: DBContext,
    after: datetime.date,
    user_id_interval: Interval,
) -> pd.DataFrame:
    return execute_query_and_return_dataframe(
        SQL(
            """
            SELECT
                user_id, type, source, value, date
            FROM
                datenspende.vitaldata
            WHERE
                user_id BETWEEN {min_id} AND {max_id} AND
                date > {date}
            ORDER BY
                user_id
            """
        ).format(
            min_id=Literal(user_id_interval["min"]),
            max_id=Literal(user_id_interval["max"]),
            date=Literal(after),
        ),
        db_context,
    )
