from postgres_helpers import DBContext, execute_sql, query_all_elements
import pytest
from psycopg2 import IntegrityError


def test_feature_name_ids_have_to_match_columns_in_feature_table(db_context: DBContext):

    with pytest.raises(IntegrityError) as e:
        execute_sql(
            db_context,
            """
            INSERT INTO
                "datenspende_derivatives"."homogenized_features_description" (description, id, is_choice) VALUES
                ('this is a broken feature that does not exist in columns', 'f12345', true);
            """,
        )
    assert (
        'new row for relation "homogenized_features_description" violates check constraint "id_matches_feature_column'
        in str(e.value)
    )

    execute_sql(
        db_context,
        """
        INSERT INTO
            "datenspende_derivatives"."homogenized_features_description" (description, id, is_choice) VALUES
            ('this feature exists in column names', 'f49', true);
        """,
    )

    feature_descriptions_from_db = query_all_elements(
        db_context,
        """
        SELECT * FROM "datenspende_derivatives"."homogenized_features_description";
        """,
    )

    assert len(feature_descriptions_from_db) == 1
