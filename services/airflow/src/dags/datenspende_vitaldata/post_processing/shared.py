import datetime
import os
from itertools import islice
from typing import Callable
from typing import List, Any

import pandas as pd
import ramda as R
from pathos.multiprocessing import Pool
from psycopg2.sql import SQL, Literal
from datetime import timedelta
from postgres_helpers import DBContext, create_db_context, teardown_db_context
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
        user_id_batches = load_user_batches(db_context, batch_size)
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


@R.curry
def extract_process_load_vital_data_for_user_batch(
    aggregator, after: datetime.date, db_parameters, user_ids: List[int]
) -> None:
    db_context = create_db_context()
    R.pipe(
        load_user_vitals_after_date(db_context, after),
        aggregator,
        upsert_pandas_dataframe_to_table_in_schema_with_db_context(
            db_context, *db_parameters
        ),
    )(user_ids)
    teardown_db_context(db_context)


@R.curry
def load_user_batches(db_context: DBContext, batchsize: int) -> List[List[int]]:
    return R.pipe(load_distinct_user_ids, split_list_into_sublist_of_size(batchsize))(
        db_context
    )


def load_distinct_user_ids(db_context: DBContext) -> List[int]:
    return R.pipe(
        execute_query_and_return_dataframe(
            SQL("SELECT DISTINCT user_id FROM datenspende.vitaldata;"),
        ),
        lambda df: df["user_id"].values,
        R.map(int),
        list,
    )(db_context)


@R.curry
def split_list_into_sublist_of_size(batchsize: int, data: List[Any]) -> List[Any]:
    elements = len(data) // batchsize
    rest = len(data) - elements * batchsize
    list_lengths = [batchsize] * elements + [rest]

    print(f"Processing {elements + 1} batches of data.")
    return [list(islice(iter(data), length)) for length in list_lengths]


@R.curry
def load_user_vitals_after_date(
    db_context: DBContext,
    after: datetime.date,
    user_ids: List[int],
) -> pd.DataFrame:
    return execute_query_and_return_dataframe(
        SQL(
            """
            SELECT
                user_id, type, source, value, date
            FROM
                datenspende.vitaldata
            WHERE
                user_id IN ({user_ids}) AND
                date > {date}
            ORDER BY
                user_id
            """
        ).format(
            user_ids=SQL(",").join([Literal(user_id) for user_id in user_ids]),
            date=Literal(after),
        ),
        db_context,
    )
