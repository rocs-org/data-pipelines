from typing import TypedDict
import psycopg2
from psycopg2.extensions import connection, cursor


class Connection(connection):
    def __deepcopy__(self, memo):
        return self


class Cursor(cursor):
    def __deepcopy__(self, memo):
        return self


psycopg2.extensions.connection = Connection
psycopg2.extensions.cursor = Cursor


class DBCredentials(TypedDict):
    user: str
    password: str
    database: str
    host: str
    port: int


class DBContext(TypedDict):
    credentials: DBCredentials
    connection: Connection


class DBContextWithCursor(TypedDict):
    credentials: DBCredentials
    connection: Connection
    cursor: Cursor
