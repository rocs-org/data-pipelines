from .db_context import DB_Context
from yoyo import read_migrations, get_backend
import pathlib
import ramda as R


def migrate(context: DB_Context):
    migrations = read_migrations(get_path_of_file() + "/migrations/")
    backend = R.pipe(get_connection_string, get_backend)(context)
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))
    return context


def get_connection_string(context: DB_Context) -> str:
    creds = context["credentials"]
    return f"postgres://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())
