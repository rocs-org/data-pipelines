from datetime import datetime, date, timedelta

import pandas as pd

from .sync_clickhouse import extract_data_between, load_data, extract_load_epoch_data


def test_extract_daily_data_from_clickhouse_returns_dataframes_with_correct_shape_and_columns(
    thryve_clickhouse_context, ch_context
):

    tables = ["raw_DynamicEpochValue"]

    start_time = datetime(2021, 9, 1)
    end_time = datetime(2022, 9, 2)

    static_data_external = extract_data_between(
        thryve_clickhouse_context, start_time, end_time, "createdAt", tables
    )

    assert static_data_external["raw_DynamicEpochValue"].shape == (1, 19)
    assert static_data_external["raw_DynamicEpochValue"]["createdAt"].values[
        0
    ] == pd.Timestamp("2021-09-01 10:14:47")

    load_data(ch_context, static_data_external)

    static_tables_local = extract_data_between(
        ch_context, start_time, end_time, "createdAt", tables
    )

    for table in tables:
        assert static_data_external[table].shape == static_tables_local[table].shape
        assert (
            static_data_external[table].columns == static_tables_local[table].columns
        ).all()


def test_el_task_loads_with_no_errors(thryve_clickhouse_context, ch_context):

    tables = ["raw_DynamicEpochValue"]
    execution_date = date(2021, 9, 2)
    context = {
        "external_table": "raw_DynamicEpochValue",
        "dag_run": {
            "conf": {
                "prefix": "",
                "CLICKHOUSE_DB": ch_context["credentials"]["database"],
                "EXTERNAL_CLICKHOUSE_DB": thryve_clickhouse_context["credentials"][
                    "database"
                ],
            }
        },
    }

    extract_load_epoch_data(
        execution_date=execution_date,
        **context,
    )
    static_tables_local = extract_data_between(
        ch_context,
        execution_date - timedelta(days=1),
        execution_date,
        "createdAt",
        tables,
    )

    assert static_tables_local["raw_DynamicEpochValue"].shape == (1, 19)
    assert static_tables_local["raw_DynamicEpochValue"]["createdAt"].values[
        0
    ] == pd.Timestamp("2021-09-01 10:14:47")
