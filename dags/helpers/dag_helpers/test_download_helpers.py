import pytest
import pandas as pd
from .download_helpers import download_csv
from dags.helpers.test_helpers import with_downloadable_csv

URL = "http://some.random.url/file.csv"
csv_content = """col1,col2,col3
1,hello,world
2,not,today
"""


@with_downloadable_csv(url=URL, content=csv_content)
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
