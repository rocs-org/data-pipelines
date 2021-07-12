from posix import environ
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import random
import string


def read_db_credentials_from_env():
    return {
        "user": os.environ["TARGET_DB_USER"],
        "password": os.environ["TARGET_DB_PW"],
        "database": os.environ["TARGET_DB"],
        "host": os.environ["TARGET_DB_HOSTNAME"],
        "port": os.environ["TARGET_DB_PORT"],
    }


def create_db_context():
    credentials = read_db_credentials_from_env()
    return {
        "cursor": psycopg2.connect(**credentials).cursor(),
        "connection": psycopg2.connect(**credentials),
        **credentials,
    }


def create_test_db_context():
    credentials = read_db_credentials_from_env()
    connection = psycopg2.connect(**credentials)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    test_db_name = get_random_string(8).lower()
    print(test_db_name)
    cursor.execute(f"CREATE DATABASE {test_db_name};")
    cursor.close()
    connection.close()
    credentials["database"] = test_db_name

    return {
        "cursor": psycopg2.connect(**credentials).cursor(),
        "connection": psycopg2.connect(**credentials),
        **credentials,
    }


def teardown_db_context(context: dict):
    print(context)
    # context["cursor"].execute(f"DROP DATABASE {context['database']}")
    context["cursor"].close()
    context["connection"].close()


def get_random_string(length):
    return "".join(random.choice(string.ascii_letters) for i in range(length))
