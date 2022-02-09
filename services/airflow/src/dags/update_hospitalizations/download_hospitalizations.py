import pandas as pd
from time import sleep


def download_hospitalizations(url):
    downloaded = False
    retries = 0
    while downloaded is False:
        try:
            data = pd.read_csv(url)
            downloaded = True
        except ConnectionResetError:
            sleep(60)
            retries += 1
            if retries > 100:
                raise TimeoutError("retried connecting 100 times without success.")
    return data
