from pytest import fixture
from .create_test_db_context import create_test_db_context, teardown_db_context


@fixture
def db_context():
    db_context = create_test_db_context()
    yield db_context
    teardown_db_context(db_context)


def test_db_connection(db_context):
    db_context["cursor"].execute(
        """CREATE TABLE test_table (
            id serial PRIMARY KEY,
            c1 varchar(255) NOT NULL,
            c2 varchar(255) NOT NULL
            );
        """
    )
    db_context["cursor"].execute(
        "INSERT INTO test_table (c1, c2) VALUES(%s, %s);",
        ("Hello", "World!"),
    )
    db_context["cursor"].execute(
        """
             SELECT * FROM test_table;
        """
    )
    res = db_context["cursor"].fetchall()[0]

    assert res[0] == 1
    assert res[1] == "Hello"
    assert res[2] == "World!"
