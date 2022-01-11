from src.lib.dag_helpers import rename_columns_convert_camel_to_snake_case

COLUMN_REPLACEMENTS = {
    "user": "user_id",
    "customer": "user_id",
    "order": "order_id",
    "value": "answer_text",
}

transform = rename_columns_convert_camel_to_snake_case(COLUMN_REPLACEMENTS)
