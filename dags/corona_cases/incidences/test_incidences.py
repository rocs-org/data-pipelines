import polars
from datetime import datetime
import ramda as R
from returns.curry import curry
from dags.database import DBContext
from typing import Any, List
from copy import deepcopy
from dags.nuts_regions_population.nuts_regions import etl_eu_regions, REGIONS_ARGS
from dags.nuts_regions_population.german_counties_more_info import (
    etl_german_counties_more_info,
    COUNTIES_ARGS,
)
from dags.corona_cases.cases import etl_covid_cases, CASES_ARGS
from io import StringIO
from .incidences import (
    calculate_incidence,
    aggregate_df_on_keys,
    get_continuous_dates,
    load_cases_data,
    load_counties_info,
)


def test_aggregate_df_sums_correctly():
    with StringIO(_TEST_DATA) as f:
        df = polars.read_csv(file=f)
    aggregate = aggregate_df_on_keys(["IdLandkreis", "Meldedatum"], df)
    print(aggregate)
    assert df["AnzahlFall"].sum() == aggregate["new_cases"].sum()
    assert df["AnzahlTodesfall"].sum() == aggregate["new_deaths"].sum()


def test_aggregate_df_results_in_unique_key_combinations():
    with StringIO(_TEST_DATA) as f:
        df = polars.read_csv(file=f)

    aggregate = aggregate_df_on_keys(["IdLandkreis", "Meldedatum"], df)

    key_combinations = list(zip(aggregate["IdLandkreis"], aggregate["Meldedatum"]))

    assert len(set(key_combinations)) == len(key_combinations)
    assert len(key_combinations) > 0


def test_get_continuous_dates_returns_list_of_serial_integers():
    with StringIO(_TEST_DATA) as f:
        dates = R.pipe(polars.read_csv, get_continuous_dates("Meldedatum"))(f)

    assert dates == [
        18324,
        18325,
        18326,
        18327,
        18328,
        18329,
        18330,
        18331,
        18332,
        18333,
        18334,
        18335,
        18336,
        18337,
        18338,
        18339,
        18340,
        18341,
        18342,
    ]


CASES_URL = "http://static-files/static/coronacases.csv"
REGIONS_URL = "http://static-files/static/NUTS2021.xlsx"
COUNTIES_URL = "http://static-files/static/alle-kreise.xlsx"


def test_calculated_incidence_matches_expected_results(db_context: DBContext):

    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, etl_eu_regions)
    replace_url_in_args_and_run_task(
        COUNTIES_URL, COUNTIES_ARGS, etl_german_counties_more_info
    )
    replace_url_in_args_and_run_task(CASES_URL, CASES_ARGS, etl_covid_cases)

    incidence_data = R.converge(
        calculate_incidence, [load_counties_info, load_cases_data]
    )("")

    test_result_data = polars.read_csv(StringIO(_TEST_RESULT_DATA))

    test_result_data["date_of_report"] = test_result_data["date_of_report"].apply(
        lambda d: datetime.strptime(d, "%Y-%m-%d").date()
    )

    # most polars methods do not support columns containing objects,
    # therefore, date_of_report has to be handled separately
    assert (
        test_result_data.drop("date_of_report").to_csv()
        == incidence_data.drop("date_of_report").to_csv()
    )

    for a, b in zip(
        incidence_data["date_of_report"], test_result_data["date_of_report"]
    ):
        assert a == b


def replace_url_in_args_and_run_task(url, args, task):
    return R.pipe(
        assoc_to_list_position(url, 0),
        R.apply(task),
    )(args)


@curry
def assoc_to_list_position(value: Any, position: int, lst: List) -> List:
    d_list = deepcopy(lst)
    d_list[position] = value
    return d_list


_TEST_DATA = """
IdLandkreis,Altersgruppe,Geschlecht,Meldedatum,Refdatum,IstErkrankungsbeginn,NeuerFall,NeuerTodesfall,NeuGenesen,AnzahlFall,AnzahlTodesfall,AnzahlGenesen
1001,A15-A34,M,2020-03-19,2020-03-13,1,0,-9,0,1,0,1
1001,A15-A34,M,2020-03-21,2020-03-13,1,0,-9,0,1,0,1
1001,A15-A34,W,2020-03-19,2020-03-16,3,0,-9,0,1,0,1
11001,A15-A34,M,2020-03-03,2020-02-28,1,0,-9,0,1,0,1
11001,A35-A59,M,2020-03-03,2020-03-01,1,0,-9,0,1,0,1
11001,A35-A59,M,2020-03-05,2020-03-02,1,0,-9,0,1,1,1
11001,A35-A59,M,2020-03-17,2020-03-03,1,0,-9,0,1,0,1
11002,A15-A34,M,2020-03-03,2020-02-28,1,0,-9,0,1,0,1
11002,A35-A59,M,2020-03-03,2020-03-01,1,0,-9,0,1,0,1
11002,A35-A59,M,2020-03-05,2020-03-02,1,0,-9,0,1,0,1
11002,A35-A59,M,2020-03-17,2020-03-03,1,0,-9,0,1,0,1
"""


_TEST_RESULT_DATA = """
location_id,location_level,date_of_report,new_cases,new_cases_last_7d,incidence_7d_per_100k,new_deaths,nuts3,population,state
1001,4,2020-10-30,1,,,0,DEF01,89934,1
1001,4,2020-10-31,3,,,0,DEF01,89934,1
1001,4,2020-11-01,0,,,0,DEF01,89934,1
1001,4,2020-11-02,0,,,0,DEF01,89934,1
1001,4,2020-11-03,0,,,0,DEF01,89934,1
1001,4,2020-11-04,0,,,0,DEF01,89934,1
1001,4,2020-11-05,0,4,4.447706,0,DEF01,89934,1
1001,4,2020-11-06,0,3,3.3357797,0,DEF01,89934,1
1001,4,2020-11-07,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-08,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-09,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-10,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-11,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-12,0,0,0.0,0,DEF01,89934,1
1001,4,2020-11-13,0,0,0.0,0,DEF01,89934,1
1002,4,2020-10-30,1,,,0,DEF02,246601,1
1002,4,2020-10-31,0,,,0,DEF02,246601,1
1002,4,2020-11-01,0,,,0,DEF02,246601,1
1002,4,2020-11-02,0,,,0,DEF02,246601,1
1002,4,2020-11-03,0,,,0,DEF02,246601,1
1002,4,2020-11-04,0,,,0,DEF02,246601,1
1002,4,2020-11-05,0,1,0.40551335,0,DEF02,246601,1
1002,4,2020-11-06,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-07,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-08,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-09,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-10,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-11,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-12,0,0,0.0,0,DEF02,246601,1
1002,4,2020-11-13,0,0,0.0,0,DEF02,246601,1
11001,4,2020-10-30,0,,,0,,,11
11001,4,2020-10-31,0,,,0,,,11
11001,4,2020-11-01,0,,,0,,,11
11001,4,2020-11-02,1,,,0,,,11
11001,4,2020-11-03,1,,,0,,,11
11001,4,2020-11-04,0,,,0,,,11
11001,4,2020-11-05,1,3,3.3357797,0,,,11
11001,4,2020-11-06,0,3,3.3357797,0,,,11
11001,4,2020-11-07,0,3,3.3357797,0,,,11
11001,4,2020-11-08,0,3,3.3357797,0,,,11
11001,4,2020-11-09,0,2,2.223853,0,,,11
11001,4,2020-11-10,0,1,1.1119266,0,,,11
11001,4,2020-11-11,0,1,1.1119266,0,,,11
11001,4,2020-11-12,0,0,0.0,0,,,11
11001,4,2020-11-13,0,0,0.0,0,,,11
11002,4,2020-10-30,0,,,0,,,11
11002,4,2020-10-31,0,,,0,,,11
11002,4,2020-11-01,0,,,0,,,11
11002,4,2020-11-02,0,,,0,,,11
11002,4,2020-11-03,0,,,0,,,11
11002,4,2020-11-04,0,,,0,,,11
11002,4,2020-11-05,0,0,0.0,0,,,11
11002,4,2020-11-06,0,0,0.0,0,,,11
11002,4,2020-11-07,0,0,0.0,0,,,11
11002,4,2020-11-08,1,1,1.1119266,0,,,11
11002,4,2020-11-09,0,1,1.1119266,0,,,11
11002,4,2020-11-10,0,1,1.1119266,0,,,11
11002,4,2020-11-11,0,1,1.1119266,0,,,11
11002,4,2020-11-12,0,1,1.1119266,0,,,11
11002,4,2020-11-13,2,3,3.3357797,0,,,11
1001,3,2020-10-30,1,,,0,DEF01,89934,1
1001,3,2020-10-31,3,,,0,DEF01,89934,1
1001,3,2020-11-01,0,,,0,DEF01,89934,1
1001,3,2020-11-02,0,,,0,DEF01,89934,1
1001,3,2020-11-03,0,,,0,DEF01,89934,1
1001,3,2020-11-04,0,,,0,DEF01,89934,1
1001,3,2020-11-05,0,4,4.447706,0,DEF01,89934,1
1001,3,2020-11-06,0,3,3.3357797,0,DEF01,89934,1
1001,3,2020-11-07,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-08,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-09,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-10,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-11,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-12,0,0,0.0,0,DEF01,89934,1
1001,3,2020-11-13,0,0,0.0,0,DEF01,89934,1
1002,3,2020-10-30,1,,,0,DEF02,246601,1
1002,3,2020-10-31,0,,,0,DEF02,246601,1
1002,3,2020-11-01,0,,,0,DEF02,246601,1
1002,3,2020-11-02,0,,,0,DEF02,246601,1
1002,3,2020-11-03,0,,,0,DEF02,246601,1
1002,3,2020-11-04,0,,,0,DEF02,246601,1
1002,3,2020-11-05,0,1,0.40551335,0,DEF02,246601,1
1002,3,2020-11-06,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-07,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-08,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-09,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-10,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-11,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-12,0,0,0.0,0,DEF02,246601,1
1002,3,2020-11-13,0,0,0.0,0,DEF02,246601,1
11000,3,2020-10-30,0,,,0,DE300,,11
11000,3,2020-10-31,0,,,0,DE300,,11
11000,3,2020-11-01,0,,,0,DE300,,11
11000,3,2020-11-02,1,,,0,DE300,,11
11000,3,2020-11-03,1,,,0,DE300,,11
11000,3,2020-11-04,0,,,0,DE300,,11
11000,3,2020-11-05,1,3,inf,0,DE300,,11
11000,3,2020-11-06,0,3,inf,0,DE300,,11
11000,3,2020-11-07,0,3,inf,0,DE300,,11
11000,3,2020-11-08,1,4,inf,0,DE300,,11
11000,3,2020-11-09,0,3,inf,0,DE300,,11
11000,3,2020-11-10,0,2,inf,0,DE300,,11
11000,3,2020-11-11,0,2,inf,0,DE300,,11
11000,3,2020-11-12,0,1,inf,0,DE300,,11
11000,3,2020-11-13,2,3,inf,0,DE300,,11
1,1,2020-10-30,2,,,0,,336535,1
1,1,2020-10-31,3,,,0,,336535,1
1,1,2020-11-01,0,,,0,,336535,1
1,1,2020-11-02,0,,,0,,336535,1
1,1,2020-11-03,0,,,0,,336535,1
1,1,2020-11-04,0,,,0,,336535,1
1,1,2020-11-05,0,5,1.4857296,0,,336535,1
1,1,2020-11-06,0,3,0.89143777,0,,336535,1
1,1,2020-11-07,0,,,0,,336535,1
1,1,2020-11-08,0,,,0,,336535,1
1,1,2020-11-09,0,,,0,,336535,1
1,1,2020-11-10,0,,,0,,336535,1
1,1,2020-11-11,0,,,0,,336535,1
1,1,2020-11-12,0,,,0,,336535,1
1,1,2020-11-13,0,,,0,,336535,1
11,1,2020-10-30,0,,,0,,,11
11,1,2020-10-31,0,,,0,,,11
11,1,2020-11-01,0,,,0,,,11
11,1,2020-11-02,1,,,0,,,11
11,1,2020-11-03,1,,,0,,,11
11,1,2020-11-04,0,,,0,,,11
11,1,2020-11-05,1,3,inf,0,,,11
11,1,2020-11-06,0,3,inf,0,,,11
11,1,2020-11-07,0,3,inf,0,,,11
11,1,2020-11-08,1,4,inf,0,,,11
11,1,2020-11-09,0,3,inf,0,,,11
11,1,2020-11-10,0,2,inf,0,,,11
11,1,2020-11-11,0,2,inf,0,,,11
11,1,2020-11-12,0,1,inf,0,,,11
11,1,2020-11-13,2,3,inf,0,,,11
0,0,2020-10-30,2,,,0,,336535,
0,0,2020-10-31,3,,,0,,336535,
0,0,2020-11-01,0,,,0,,336535,
0,0,2020-11-02,1,,,0,,336535,
0,0,2020-11-03,1,,,0,,336535,
0,0,2020-11-04,0,,,0,,336535,
0,0,2020-11-05,1,8,2.3771672,0,,336535,
0,0,2020-11-06,0,6,1.7828755,0,,336535,
0,0,2020-11-07,0,3,0.89143777,0,,336535,
0,0,2020-11-08,1,4,1.1885836,0,,336535,
0,0,2020-11-09,0,3,0.89143777,0,,336535,
0,0,2020-11-10,0,2,0.5942918,0,,336535,
0,0,2020-11-11,0,2,0.5942918,0,,336535,
0,0,2020-11-12,0,1,0.2971459,0,,336535,
0,0,2020-11-13,2,3,0.89143777,0,,336535,
"""
