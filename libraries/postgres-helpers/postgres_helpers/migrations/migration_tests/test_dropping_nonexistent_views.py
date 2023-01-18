from postgres_helpers import execute_sql, DBContext
from postgres_helpers.migrations.migrations import migrate_until, migrate


def test_migrations_with_missing_views(db_context: DBContext):
    migrate_until(db_context, 52)
    execute_sql(
        db_context,
        """
            DROP MATERIALIZED VIEW IF EXISTS
                datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection;
        """,
    )
    execute_sql(
        db_context,
        """
            CREATE TABLE IF NOT EXISTS
            datenspende_derivatives.vitals_standardized_by_date_and_user_before_infection (
                user_id text
                )
            """,
    )
    migrate(db_context)
