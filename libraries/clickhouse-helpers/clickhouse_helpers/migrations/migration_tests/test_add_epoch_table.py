import pandas as pd
from io import StringIO
from clickhouse_helpers import DBContext, insert_dataframe, query_dataframe, execute_sql


def test_epoch_table_schema_fits_data(db_context: DBContext):
    df = pd.read_csv(
        StringIO(
            """customer,type,valueType,doubleValue,longValue,booleanValue,timezoneOffset,startTimestamp,endTimestamp,createdAt,source
95632,1000,0,57.0,,,120,1593657859000,1593657930000,1593718330577,6
95632,1000,0,25.0,,,120,1593657867000,1593657877000,1593718330577,6
95632,3000,1,,62,,120,1593658173000,1593658173000,1593718330331,6
95632,3000,1,,62,,120,1593658295000,1593658295000,1593718330329,6
95632,3000,1,,63,,120,1593658550000,1593658550000,1593718330329,6
95632,3000,1,,62,,120,1593659031000,1593659031000,1593718330329,6
95632,3000,1,,60,,120,1593659097000,1593659097000,1593718330329,6
95632,3000,1,,64,,120,1593659525000,1593659525000,1593718330328,6
95632,3000,2,,62,,120,1593659784000,1593659784000,1593718330328,6
    """
        ),
        sep=",",
    ).replace({float("nan"): None})

    insert_dataframe(db_context, "vital_data_epoch", df)

    res = query_dataframe(db_context, "SELECT * FROM vital_data_epoch")

    print(len(res))
    assert (res.values == df.values).all()

    # inserting data a second time writes to table
    insert_dataframe(db_context, "vital_data_epoch", df)
    res = query_dataframe(db_context, "SELECT * FROM vital_data_epoch")
    assert len(res) == 2 * len(df)

    # after optimizing the table, duplicates are gone.
    execute_sql(db_context, """OPTIMIZE TABLE vital_data_epoch""")

    res = query_dataframe(db_context, "SELECT * FROM vital_data_epoch")
    assert len(res) == len(df)
