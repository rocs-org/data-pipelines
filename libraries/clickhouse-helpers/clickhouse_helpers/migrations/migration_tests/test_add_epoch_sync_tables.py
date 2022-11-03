import os
import pytest
import pandas as pd
from clickhouse_helpers.db_context import (
    create_test_db_context,
    teardown_test_db_context,
)
from clickhouse_helpers import DBContext, query_dataframe, migrate


@pytest.fixture
def impatient_db_context() -> DBContext:
    try:
        context = create_test_db_context(send_receive_timeout=3)
        migrate(context)
        yield context
    finally:
        teardown_test_db_context(context)


@pytest.mark.parametrize(
    "table_name,str_columns,int_columns,float_columns,date_columns",
    [
        (
            "B2CCustomer",
            [
                "id_email",
                "id_emailLowerCase",
                "activationCode",
                "partnerUserID",
                "firstName",
                "lastName",
                "phoneNumber",
                "password_jbcrypt",
                "diviceID",
                "appVersion",
                "profilePicture_body",
                "address_street",
                "address_postalCode",
                "address_city",
                "pushErrors_0",
                "pushErrors_1",
                "pushErrors_2",
            ],
            [
                "this",
                "catch",
                "company",
                "salutation",
                "language",
                "sharedData_emergencies",
                "sharedData_anomalies",
                "sharedData_location",
                "sharedData_activityData",
                "sharedData_friends",
                "sharedData_information",
                "notificatSettings_emergen",
                "notificatSettings_anomali",
                "notificatSettings_informa",
                "batteryWarning",
                "useActivDataForAnomaDetec",
                "deadManSwitchTriggered",
                "deadManSwitchPeriod",
                "refereceGroup",
                "creationTimestamp",
                "weight",
                "height",
                "profilePicture_lastModifi",
                "address_country",
                "pushErrors_Len",
                "termsAccepted",
                "privacyPolicyAccepted",
                "askedForDeletion",
                "blocked",
                "confirmed",
                "acceptedPolicyVersion",
            ],
            [],
            [],
        ),
        (
            "DailyDynamicValue",
            [
                "customer",
                "valueType",
                "booleanValue",
                "stringValue",
                "generation",
                "trustworthiness",
                "medicalGrade",
                "userReliability",
                "outdated",
            ],
            [
                "type",
                "source",
                "longValue",
                "chronologicalExactness",
                "timezoneOffset",
            ],
            ["doubleValue"],
            ["day", "dateValue", "createdAt"],
        ),
        (
            "DynamicEpochValue",
            [
                "valueType",
                "booleanValue",
                "generation",
                "trustworthiness",
                "medicalGrade",
                "userReliability",
                "outdated",
                "stringValue",
            ],
            [],
            [],
            ["startTimestamp", "endTimestamp", "createdAt"],
        ),
        (
            "DailyDynamicValueType",
            ["code", "name_de", "name_en", "name_es", "name_fr", "name_it"],
            ["this", "catch", "id", "valueType", "level"],
            [],
            [],
        ),
        (
            "DynamicDataSource",
            [
                "code",
                "name_de",
                "name_en",
                "name_es",
                "name_fr",
                "name_it",
                "logo_body",
            ],
            [
                "this",
                "catch",
                "id",
                "enabled",
                "requiresPing",
                "logo_contentType",
                "logo_lastModified",
                "enableLog",
            ],
            [],
            [],
        ),
        (
            "DynamicValueType",
            ["code", "name_de", "name_en", "name_es", "name_fr", "name_it"],
            ["this", "catch", "id", "valueType", "level"],
            [],
            [],
        ),
    ],
)
def test_data_fits_tables(
    impatient_db_context: DBContext,
    table_name: str,
    str_columns,
    int_columns,
    float_columns,
    date_columns,
):
    df = read_csv_with_types(
        str_columns=str_columns,
        int_columns=int_columns,
        date_columns=date_columns,
        float_columns=float_columns,
        file_name=f"{table_name}.csv",
    )

    columns = ", ".join(df.columns)
    query = f"INSERT INTO raw_{table_name} ({columns}) VALUES"
    params = list(df.values.T)

    impatient_db_context["connection"].execute(
        query, params=params, columnar=True, types_check=False
    )

    df_from_db = query_dataframe(
        impatient_db_context, f"SELECT * FROM raw_{table_name}"
    )

    assert set(list(df_from_db.columns.values)) == set(list(df.columns.values))


def read_csv_with_types(
    str_columns, int_columns, float_columns, date_columns, file_name
):
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), file_name),
        dtype={
            **{key: str for key in str_columns},
            **{key: pd.Int64Dtype() for key in int_columns},
            **{key: float for key in float_columns},
        },
        parse_dates=date_columns,
        index_col=0,
    ).replace(
        {
            float("nan"): None,
            "nan": None,
            "True": "true",
            "False": "false",
        }
    )

    timestamp_columns = date_columns
    for column in timestamp_columns:
        df[column] = df[column].apply(pd.to_datetime).dt.tz_localize(None)

    return df


# read DynamicEpochValue data from csv in this directory
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

DAILY_DYNAMIC_VALUE_DATA = pd.read_csv(
    os.path.join(__location__, "DailyDynamicValue.csv")
)
DAILY_DYNAMIC_VALUE_TYPE_DATA = pd.read_csv(
    os.path.join(__location__, "DailyDynamicValueType.csv")
)
DYNAMIC_DATA_SOURCE_DATA = pd.read_csv(
    os.path.join(__location__, "DynamicDataSource.csv")
)
DYNAMIC_EPOCH_VALUE_DATA = pd.read_csv(
    os.path.join(__location__, "DynamicEpochValue.csv")
)
DYNAMIC_VALUE_TYPE_DATA = pd.read_csv(
    os.path.join(__location__, "DynamicValueType.csv")
)
