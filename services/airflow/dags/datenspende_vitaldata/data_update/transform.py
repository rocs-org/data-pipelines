import polars
import ramda as R

from dags.database import camel_case_to_snake_case


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
    "type": "vital_id",
    "long_value": "value",
    "source": "device_id",
}

transform = transform_columns(COLUMN_REPLACEMENTS)
