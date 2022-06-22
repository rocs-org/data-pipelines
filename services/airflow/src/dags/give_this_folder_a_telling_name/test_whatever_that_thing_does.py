import pandas as pd

from .whatever_that_thing_does_dag import some_function_with_tests

URL = "http://static-files/static/test.csv"


def test_write_some_tests_for_your_code_here(db_context):
    # db_context gives you a handle to the database for testing.
    df = some_function_with_tests(pd.DataFrame())
    assert df.values == pd.DataFrame().values
