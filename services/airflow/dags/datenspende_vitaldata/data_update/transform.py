import polars
import ramda as R

from database import camel_case_to_snake_case


@R.curry
def replace(mapping: dict, elements: list) -> list:
    return [mapping.get(n, n) for n in elements]


@R.curry
def transform_columns(column_mapping: dict, data: polars.DataFrame) -> polars.DataFrame:
    data.columns = R.pipe(R.map(camel_case_to_snake_case), replace(column_mapping))(
        data.columns
    )
    return data


COLUMN_REPLACEMENTS = {
    "user": "user_id",
    "customer": "user_id",
    "day": "date",
    "long_value": "value",
}

transform = transform_columns(COLUMN_REPLACEMENTS)
