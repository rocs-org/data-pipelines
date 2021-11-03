from database import DBContext, execute_sql, query_all_elements
import pytest
from psycopg2 import IntegrityError


def test_population_entry_needs_nuts_region_to_exist(db_context: DBContext):
    with pytest.raises(IntegrityError) as e:
        execute_sql(
            db_context,
            "INSERT INTO censusdata.population (number, nuts, agegroup, sex, year) VALUES(%s, %s, %s, %s, %s)",
            (1234, "DE1", "Y35-39", "W", 2015),
        )
    assert 'Key (nuts)=(DE1) is not present in table "nuts"' in str(e.value)

    execute_sql(
        db_context,
        "INSERT INTO censusdata.nuts (level, geo, name, country_id) VALUES(%s, %s, %s, %s)",
        (1, "DE1", "Deutschland", "10"),
    )
    execute_sql(
        db_context,
        "INSERT INTO censusdata.population (number, nuts, agegroup, sex, year) VALUES(%s, %s, %s, %s, %s)",
        (1234, "DE1", "Y35-39", "W", 2015),
    )
    res = query_all_elements(
        db_context,
        "SELECT number, nuts, agegroup, sex, year FROM censusdata.population",
    )
    assert len(res) == 1
    assert res[0] == (1234, "DE1", "Y35-39", "W", 2015)
