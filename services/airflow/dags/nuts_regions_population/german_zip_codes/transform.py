import pandas as pd
import ramda as R

from dags.nuts_regions_population.german_counties_more_info.transform import (
    rename_columns_with_mapping,
)


def remove_single_quotes_from_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda string: string.replace("'", ""))


transform = R.pipe(
    rename_columns_with_mapping({"NUTS3": "nuts", "CODE": "zip_code"}),
    remove_single_quotes_from_strings,
)
