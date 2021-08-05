"""
test migration
"""

from yoyo import step


steps = [
    step(
        "CREATE TABLE test_table (id SERIAL PRIMARY KEY, col1 INT, col2 VARCHAR(255), col3 VARCHAR(255));",
        "DROP TABLE test_table;",
    )
]
