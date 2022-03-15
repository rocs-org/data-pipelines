import polars
import ramda as R

from postgres_helpers import camel_case_to_snake_case


@R.curry
def replace(mapping: dict, elements: list) -> list:
    return [mapping.get(n, n) for n in elements]


@R.curry
def rename_columns_convert_camel_to_snake_case(
    column_mapping: dict, data: polars.DataFrame
) -> polars.DataFrame:
    data.columns = R.pipe(R.map(camel_case_to_snake_case), replace(column_mapping))(
        data.columns
    )
    return data
