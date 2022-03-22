import pandas as pd
import datetime
from postgres_helpers import DBContext
from src.dags.datenspende_vitaldata.post_processing.shared import load_per_user_data
from src.dags.datenspende_vitaldata.post_processing.test_pivot_tables import (
    setup_vitaldata_in_db,
)


def test_load_per_user_data_loads_vitals_for_existing_users(pg_context: DBContext):
    setup_vitaldata_in_db()

    for user_data in list(load_per_user_data(pg_context)):
        print(user_data.to_dict())
        assert isinstance(user_data, pd.DataFrame)
        assert (
            user_data.to_dict() == FIRST_TEST_USER_DATA
            or user_data.to_dict() == SECOND_TEST_USER_DATA
        )


FIRST_TEST_USER_DATA = {
    "user_id": {0: 200, 1: 200, 2: 200, 3: 200, 4: 200},
    "type": {0: 9, 1: 65, 2: 43, 3: 52, 4: 53},
    "source": {0: 6, 1: 6, 2: 6, 3: 6, 4: 6},
    "date": {
        0: datetime.date(2021, 10, 21),
        1: datetime.date(2021, 10, 21),
        2: datetime.date(2021, 10, 22),
        3: datetime.date(2021, 10, 22),
        4: datetime.date(2021, 10, 22),
    },
    "value": {0: 3600, 1: 70, 2: 400, 3: 1634854000, 4: 1634879000},
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
        0: 9,
        1: 43,
        2: 52,
        3: 53,
        4: 65,
        5: 9,
        6: 43,
        7: 52,
        8: 53,
        9: 65,
        10: 9,
        11: 43,
        12: 52,
        13: 53,
        14: 65,
    },
    "source": {
        0: 6,
        1: 3,
        2: 3,
        3: 3,
        4: 3,
        5: 6,
        6: 3,
        7: 3,
        8: 3,
        9: 3,
        10: 6,
        11: 3,
        12: 3,
        13: 3,
        14: 3,
    },
    "date": {
        0: datetime.date(2021, 10, 26),
        1: datetime.date(2021, 10, 23),
        2: datetime.date(2021, 10, 23),
        3: datetime.date(2021, 10, 23),
        4: datetime.date(2021, 10, 23),
        5: datetime.date(2021, 10, 27),
        6: datetime.date(2021, 10, 24),
        7: datetime.date(2021, 10, 24),
        8: datetime.date(2021, 10, 24),
        9: datetime.date(2021, 10, 24),
        10: datetime.date(2021, 10, 28),
        11: datetime.date(2021, 10, 25),
        12: datetime.date(2021, 10, 25),
        13: datetime.date(2021, 10, 25),
        14: datetime.date(2021, 10, 25),
    },
    "value": {
        0: 4600,
        1: 500,
        2: 1634936000,
        3: 1634965000,
        4: 50,
        5: 4600,
        6: 500,
        7: 1634936000,
        8: 1634965000,
        9: 50,
        10: 4600,
        11: 500,
        12: 1634936000,
        13: 1634965000,
        14: 50,
    },
}
