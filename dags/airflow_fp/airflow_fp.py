from typing import Any
from airflow.models.taskinstance import TaskInstance
import ramda as R
from pandas import DataFrame


def pull_execute_push(origin_key: str, target_key: str, foo):
    def wrapper(ti: TaskInstance, *args):
        data = ti.xcom_pull(key=origin_key)
        ti.xcom_push(key=target_key, value=foo(*args, data))

    return wrapper


def pull_execute(origin_key: str, foo):
    def wrapper(ti: TaskInstance, *args):
        data = ti.xcom_pull(key=origin_key)
        foo(*args, data)

    return wrapper


def execute_push(target_key: str, foo):
    def wrapper(*args, ti: TaskInstance = None):
        print("args are: ", args)
        if ti is not None:
            ti.xcom_push(key=target_key, value=foo(*args))

    return wrapper


def execute_push_df(target_key: str, foo):
    def wrapper(*args, ti: TaskInstance = None):
        df: DataFrame = foo(*args)
        if ti is not None:
            push(ti, target_key, df.to_json())

    return wrapper


R.curry


def push(ti: TaskInstance, key: str, value: Any):
    ti.xcom_push(key=key, value=value)
