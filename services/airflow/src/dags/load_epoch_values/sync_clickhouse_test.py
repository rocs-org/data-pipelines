import os
import pytest
import pandas as pd
from clickhouse_helpers import migrate, create_test_db_context, teardown_test_db_context
from .sync_clickhouse import extract_static_data, load_static_data


@pytest.fixture
def thryve_clickhouse_context():
    try:
        context = create_test_db_context(env_prefix="EXTERNAL_CLICKHOUSE")
        migrate(context)

        for (
            table,
            str_columns,
            int_columns,
            float_columns,
            date_columns,
        ) in DATA_FILE_INFO:
            df = read_csv_with_types(
                str_columns, int_columns, float_columns, date_columns, f"{table}.csv"
            )

            columns = ", ".join(df.columns)
            query = f"INSERT INTO raw_{table} ({columns}) VALUES"
            params = list(df.values.T)

            context["connection"].execute(
                query, params=params, columnar=True, types_check=False
            )

        yield context
    finally:
        teardown_test_db_context(context)


def test_extract_static_data_from_clickhouse_returns_dataframes_with_correct_shape_and_columns(
    thryve_clickhouse_context, ch_context
):
    static_tables = ["DailyDynamicValueType", "DynamicDataSource", "DynamicValueType"]

    static_data_external = extract_static_data(thryve_clickhouse_context, static_tables)
    assert static_data_external["DailyDynamicValueType"].shape == (2, 6)
    assert static_data_external["DynamicDataSource"].shape == (2, 8)
    assert static_data_external["DynamicValueType"].shape == (2, 5)

    load_static_data(ch_context, static_data_external)

    static_tables_local = extract_static_data(ch_context, static_tables)

    for table in static_tables:
        assert static_data_external[table].shape == static_tables_local[table].shape
        assert static_data_external[table].columns == static_tables_local[table].columns

    assert False


def read_csv_with_types(
    str_columns, int_columns, float_columns, date_columns, file_name
):
    print(date_columns)
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
        try:
            df[column] = df[column].apply(pd.to_datetime).dt.tz_localize(None)
        except AttributeError as e:
            print(f"Error parsing {column} column: {e}")
            print(df[column])
            if set(df[column].unique()) == {None}:
                print(f"Column {column} is all nulls, skipping")
                continue
    return df


DATA_FILE_INFO = [
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
        ["code", "name_de", "name_en", "name_es", "name_fr", "name_it", "logo_body"],
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
]
