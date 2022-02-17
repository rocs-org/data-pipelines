import typer

from . import teardown_db_context, migrate
from .db_context import create_context_from_credentials


def run_migrations():
    typer.run(migrate_cli)


def migrate_cli(
    host: str = typer.Option("", help="Hostname of clickhouse instance"),
    port: str = typer.Option(
        "", help="Port of native clickhouse client on clickhouse instance"
    ),
    user: str = typer.Option("", help="username of default clickhouse user"),
    password: str = typer.Option("", help="password of default clickhouse user"),
    database: str = typer.Option("", help="name of database to apply migrations to"),
):

    credentials = {
        "database": database,
        "host": host,
        "port": port,
        "user": user,
        "password": password,
    }
    context = create_context_from_credentials(credentials)
    migrate(context)
    teardown_db_context(context)
