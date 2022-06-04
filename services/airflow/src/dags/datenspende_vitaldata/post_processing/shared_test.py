import datetime

from postgres_helpers import DBContext
from src.dags.datenspende_vitaldata.post_processing.pivot_tables_test import (
    setup_vitaldata_in_db,
)
from src.dags.datenspende_vitaldata.post_processing.shared import (
    load_user_vitals_after_date,
    load_min_user_id,
    load_max_user_id,
    get_user_id_intervals,
    Interval,
)


def test_get_user_id_intervals(pg_context: DBContext):
    setup_vitaldata_in_db()
    assert get_user_id_intervals(pg_context, 50) == [
        {"min": 100, "max": 149},
        {"min": 150, "max": 199},
        {"min": 200, "max": 200},
    ]


def test_load_min_max_user_id(pg_context: DBContext):
    setup_vitaldata_in_db()
    assert load_min_user_id(pg_context) == 100
    assert load_max_user_id(pg_context) == 200


def test_load_user_vitals_loads_vitals_for_multiple_user_ids(pg_context: DBContext):
    setup_vitaldata_in_db()
    user_id_interval: Interval = {"min": 100, "max": 200}
    vitals = load_user_vitals_after_date(datetime.date(2020, 1, 1), user_id_interval)
    assert (vitals["user_id"].unique() == [100, 200]).all()
    assert (
        load_user_vitals_after_date(
            datetime.date(2020, 1, 1), {"min": 200, "max": 200}
        ).to_dict()
        == FIRST_TEST_USER_DATA
    )
    assert (
        load_user_vitals_after_date(
            datetime.date(2020, 1, 1), {"min": 100, "max": 100}
        ).to_dict()
        == SECOND_TEST_USER_DATA
    )


FIRST_TEST_USER_DATA = {
    "user_id": {
        0: 200,
        1: 200,
        2: 200,
        3: 200,
        4: 200,
        5: 200,
        6: 200,
        7: 200,
        8: 200,
        9: 200,
        10: 200,
        11: 200,
        12: 200,
        13: 200,
        14: 200,
        15: 200,
        16: 200,
        17: 200,
        18: 200,
        19: 200,
    },
    "type": {
        0: 9,
        1: 65,
        2: 43,
        3: 52,
        4: 53,
        5: 43,
        6: 52,
        7: 53,
        8: 65,
        9: 43,
        10: 52,
        11: 53,
        12: 65,
        13: 43,
        14: 52,
        15: 53,
        16: 65,
        17: 9,
        18: 9,
        19: 9,
    },
    "source": {
        0: 6,
        1: 6,
        2: 6,
        3: 6,
        4: 6,
        5: 3,
        6: 3,
        7: 3,
        8: 3,
        9: 3,
        10: 3,
        11: 3,
        12: 3,
        13: 3,
        14: 3,
        15: 3,
        16: 3,
        17: 6,
        18: 6,
        19: 6,
    },
    "value": {
        0: 3600,
        1: 71,
        2: 400,
        3: 1634854000,
        4: 1634879000,
        5: 501,
        6: 1634936001,
        7: 1634965001,
        8: 51,
        9: 501,
        10: 1634936001,
        11: 1634965001,
        12: 51,
        13: 501,
        14: 1634936001,
        15: 1634965001,
        16: 51,
        17: 4601,
        18: 4601,
        19: 4601,
    },
    "date": {
        0: datetime.date(2021, 10, 21),
        1: datetime.date(2021, 10, 21),
        2: datetime.date(2021, 10, 22),
        3: datetime.date(2021, 10, 22),
        4: datetime.date(2021, 10, 22),
        5: datetime.date(2021, 10, 23),
        6: datetime.date(2021, 10, 23),
        7: datetime.date(2021, 10, 23),
        8: datetime.date(2021, 10, 23),
        9: datetime.date(2021, 10, 24),
        10: datetime.date(2021, 10, 24),
        11: datetime.date(2021, 10, 24),
        12: datetime.date(2021, 10, 24),
        13: datetime.date(2021, 10, 25),
        14: datetime.date(2021, 10, 25),
        15: datetime.date(2021, 10, 25),
        16: datetime.date(2021, 10, 25),
        17: datetime.date(2021, 10, 26),
        18: datetime.date(2021, 10, 27),
        19: datetime.date(2021, 10, 28),
    },
}
SECOND_TEST_USER_DATA = {
    "user_id": {
        0: 100,
        1: 100,
        2: 100,
        3: 100,
        4: 100,
        5: 100,
        6: 100,
        7: 100,
        8: 100,
        9: 100,
        10: 100,
        11: 100,
        12: 100,
        13: 100,
        14: 100,
    },
    "type": {
        0: 43,
        1: 52,
        2: 53,
        3: 65,
        4: 43,
        5: 52,
        6: 53,
        7: 65,
        8: 43,
        9: 52,
        10: 53,
        11: 65,
        12: 9,
        13: 9,
        14: 9,
    },
    "source": {
        0: 3,
        1: 3,
        2: 3,
        3: 3,
        4: 3,
        5: 3,
        6: 3,
        7: 3,
        8: 3,
        9: 3,
        10: 3,
        11: 3,
        12: 6,
        13: 6,
        14: 6,
    },
    "value": {
        0: 500,
        1: 1634936000,
        2: 1634965000,
        3: 50,
        4: 500,
        5: 1634936000,
        6: 1634965000,
        7: 50,
        8: 500,
        9: 1634936000,
        10: 1634965000,
        11: 50,
        12: 4600,
        13: 4600,
        14: 4600,
    },
    "date": {
        0: datetime.date(2021, 10, 23),
        1: datetime.date(2021, 10, 23),
        2: datetime.date(2021, 10, 23),
        3: datetime.date(2021, 10, 23),
        4: datetime.date(2021, 10, 24),
        5: datetime.date(2021, 10, 24),
        6: datetime.date(2021, 10, 24),
        7: datetime.date(2021, 10, 24),
        8: datetime.date(2021, 10, 25),
        9: datetime.date(2021, 10, 25),
        10: datetime.date(2021, 10, 25),
        11: datetime.date(2021, 10, 25),
        12: datetime.date(2021, 10, 26),
        13: datetime.date(2021, 10, 27),
        14: datetime.date(2021, 10, 28),
    },
}
