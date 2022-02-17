import ramda as R

from clickhouse_helpers import DBContext, query_dataframe
from .el_epoch_data import el_epoch_data

URL = "http://static-files/thryve/exportEpoch.7z"

EPOCH_TABLE = "vital_data_epoch"


def test_load_writes_all_epoch_frames_to_database(ch_context: DBContext):
    el_epoch_data(URL, EPOCH_TABLE)

    assert (
        number_of_elements_returned_from(ch_context)(f"SELECT * FROM {EPOCH_TABLE}")
        == 300
    )

    # Running the pipeline a second time does not input new data
    el_epoch_data(URL, EPOCH_TABLE)

    assert (
        number_of_elements_returned_from(ch_context)(f"SELECT * FROM {EPOCH_TABLE}")
        == 300
    )


@R.curry
def number_of_elements_returned_from(context: DBContext):
    return R.pipe(query_dataframe(context), len)
