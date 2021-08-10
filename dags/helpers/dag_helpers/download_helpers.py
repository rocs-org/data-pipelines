import io

import ramda as R
import requests
from pandas import DataFrame, read_csv


def download_csv(url: str) -> DataFrame:
    return R.pipe(
        R.try_catch(
            requests.get,
            lambda err, url: _raise(
                FileNotFoundError(f"Cannot find file at {url} \n trace: \n {err}")
            ),
        ),
        R.prop("content"),
        R.invoker(1, "decode")("utf-8"),
        io.StringIO,
        read_csv,
    )(url)


def _raise(ex: Exception):
    raise ex
