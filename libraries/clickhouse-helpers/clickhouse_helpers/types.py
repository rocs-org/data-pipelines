from typing import TypedDict
from clickhouse_driver import Client
from clickhouse_driver.connection import Connection


Client.__deepcopy__ = lambda self, memo: self


Connection.__deepcopy__ = lambda self, memo: self


class DBCredentials(TypedDict):
    user: str
    password: str
    database: str
    host: str
    port: int


class DBContext(TypedDict):
    credentials: DBCredentials
    connection: Client
