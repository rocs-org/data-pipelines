import subprocess

import pandas as pd

from . import (
    create_test_db_context,
    insert_dataframe,
    query_dataframe,
    teardown_test_db_context,
)


def test_migrations_cli_runs_migrations_on_passed_in_connection():
    context = create_test_db_context()
    credentials = context["credentials"]

    args = [
        "poetry",
        "run",
        "migrate-clickhouse",
        "--host",
        credentials["host"],
        "--port",
        str(credentials["port"]),
        "--user",
        credentials["user"],
        "--password",
        credentials["password"],
        "--database",
        credentials["database"],
    ]

    print(args)

    process = subprocess.Popen(args)
    process.communicate()

    assert process.returncode == 0

    insert_dataframe(
        context,
        "test_table",
        TEST_DF.set_index("id"),
    )

    res = query_dataframe(
        context,
        f"SELECT * FROM {context['credentials']['database']}.test_table;",
    )

    assert (res.values == TEST_DF.values).all()

    teardown_test_db_context(context)


TEST_DF = pd.DataFrame(
    {"id": [1, 2], "col1": [3, 4], "col2": ["a", "b"], "col3": ["c", "d"]}
)
