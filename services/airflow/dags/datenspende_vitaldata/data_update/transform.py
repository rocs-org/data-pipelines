from dags.helpers.dag_helpers import rename_columns_convert_camel_to_snake_case

COLUMN_REPLACEMENTS = {
    "user": "user_id",
    "customer": "user_id",
    "day": "date",
    "long_value": "value",
}

transform = rename_columns_convert_camel_to_snake_case(COLUMN_REPLACEMENTS)
