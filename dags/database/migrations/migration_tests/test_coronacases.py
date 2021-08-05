from dags.database import execute_sql, DBContext, query_all_elements


def test_corona_cases(db_context: DBContext):
    table = "coronacases.german_counties_more_info"
    execute_sql(
        db_context,
        """INSERT INTO coronacases.german_counties_more_info (
            stateid,
            state,
            countyid,
            county,
            agegroup,
            date_cet,
            ref_date_cet,
            ref_date_is_symptom_onset
        ) VALUES (
            1,
            1,
            1,
            'Berlin',
            1,
            to_timestamp('20/8/2013 14:52:49', 'DD/MM/YYYY hh24:mi:ss')::timestamp,
            to_timestamp('20/8/2013 14:52:49', 'DD/MM/YYYY hh24:mi:ss')::timestamp,
            true
        )""",
    )
    res = query_all_elements(db_context, f"SELECT * from {table}")
    assert len(res) == 1
