import pathlib

from clickhouse_migrate.migrate import migrate as ch_migrate

from clickhouse_helpers.types import DBContext


def migrate(context: DBContext):
    credentials = context["credentials"]
    migration_home = get_path_of_file() + "/migration_files"
    ch_migrate(
        credentials["database"],
        migration_home,
        db_host=credentials["host"],
        db_user=credentials["user"],
        db_password=credentials["password"],
        db_port=credentials["port"],
    )
    return context


def get_connection_string(context: DBContext) -> str:
    creds = context["credentials"]
    return f"postgres://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())
