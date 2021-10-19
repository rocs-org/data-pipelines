import pytest
import pandas as pd
from .download_helpers import download_csv

URL = "http://static-files/static/test.csv"


def test_download_csv():
    result = download_csv(URL)

    assert result.equals(
        pd.DataFrame(
            columns=["col1", "col2", "col3"],
            data=[[1, "hello", "world"], [2, "not", "today"]],
        )
    )

    with pytest.raises(FileNotFoundError) as exception_info:
        download_csv("http://the.wrong.url/file.csv")

    assert "Cannot find file at" in str(exception_info.value)
