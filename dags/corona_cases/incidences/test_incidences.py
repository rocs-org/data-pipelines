import polars
import pandas.api.types as ptypes
import ramda as R
from returns.curry import curry
from dags.database import DBContext
from typing import Any, List
from copy import deepcopy
from dags.nuts_regions_population.nuts_regions import regions_task, REGIONS_ARGS
from dags.nuts_regions_population.german_counties_more_info import (
    german_counties_more_info_etl,
    COUNTIES_ARGS,
)
from dags.corona_cases.cases import covid_cases_etl, CASES_ARGS
import tempfile
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
    print(dates)
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

# note: this function will fail when population data changes (e.g. in 2022).
# if that happens, just generate new _TEST_RESULT_DATA with the new population data
def test_calculated_incidence_matches_expected_results(db_context: DBContext):

    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, regions_task)
    replace_url_in_args_and_run_task(
        COUNTIES_URL, COUNTIES_ARGS, german_counties_more_info_etl
    )
    replace_url_in_args_and_run_task(CASES_URL, CASES_ARGS, covid_cases_etl)

    # make file-like object to be read by polars
    incidence_data = R.converge(
        calculate_incidence, [load_counties_info, load_cases_data]
    )("")

    print(incidence_data)

    # let polars write the result to a temporary file
    # so we can compare strings (unfortunately, polars does not write
    # into file-like objects, it only opens file by name)
    with tempfile.NamedTemporaryFile(mode="r", suffix=".csv") as f2:
        incidence_data.to_csv(f2.name)
        result_data = f2.read()

        print(result_data)
        assert result_data == _TEST_RESULT_DATA


def test_calculated_incidences_have_correct_types(db_context: DBContext):
    replace_url_in_args_and_run_task(REGIONS_URL, REGIONS_ARGS, regions_task)
    replace_url_in_args_and_run_task(
        COUNTIES_URL, COUNTIES_ARGS, german_counties_more_info_etl
    )
    replace_url_in_args_and_run_task(CASES_URL, CASES_ARGS, covid_cases_etl)

    # make file-like object to be read by polars
    incidence_data = R.converge(
        calculate_incidence, [load_counties_info, load_cases_data]
    )("")

    assert ptypes.is_integer_dtype(incidence_data["location_id"])
    assert ptypes.is_integer_dtype(incidence_data["location_level"])
    assert ptypes.is_datetime64_dtype(incidence_data["date_of_report"])
    assert ptypes.is_integer_dtype(incidence_data["new_cases"])
    assert ptypes.is_float_dtype(incidence_data["new_cases_last_7d"])
    assert ptypes.is_float_dtype(incidence_data["incidence_7d_per_100k"])
    assert ptypes.is_integer_dtype(incidence_data["new_deaths"])
    assert ptypes.is_string_dtype(incidence_data["nuts3"])
    assert ptypes.is_float_dtype(incidence_data["population"])
    assert ptypes.is_float_dtype(incidence_data["state"])


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


_TEST_RESULT_DATA = """,location_id,location_level,date_of_report,new_cases,new_cases_last_7d,incidence_7d_per_100k,new_deaths,nuts3,population,state
0,1001,4,2020-10-30,1,,,0,DEF01,89934.0,1.0
1,1001,4,2020-10-31,3,,,0,DEF01,89934.0,1.0
2,1001,4,2020-11-01,0,,,0,DEF01,89934.0,1.0
3,1001,4,2020-11-02,0,,,0,DEF01,89934.0,1.0
4,1001,4,2020-11-03,0,,,0,DEF01,89934.0,1.0
5,1001,4,2020-11-04,0,,,0,DEF01,89934.0,1.0
6,1001,4,2020-11-05,0,4.0,4.44770622253418,0,DEF01,89934.0,1.0
7,1001,4,2020-11-06,0,3.0,3.3357796669006348,0,DEF01,89934.0,1.0
8,1001,4,2020-11-07,0,0.0,0.0,0,DEF01,89934.0,1.0
9,1001,4,2020-11-08,0,0.0,0.0,0,DEF01,89934.0,1.0
10,1001,4,2020-11-09,0,0.0,0.0,0,DEF01,89934.0,1.0
11,1001,4,2020-11-10,0,0.0,0.0,0,DEF01,89934.0,1.0
12,1001,4,2020-11-11,0,0.0,0.0,0,DEF01,89934.0,1.0
13,1001,4,2020-11-12,0,0.0,0.0,0,DEF01,89934.0,1.0
14,1001,4,2020-11-13,0,0.0,0.0,0,DEF01,89934.0,1.0
15,1002,4,2020-10-30,1,,,0,DEF02,246601.0,1.0
16,1002,4,2020-10-31,0,,,0,DEF02,246601.0,1.0
17,1002,4,2020-11-01,0,,,0,DEF02,246601.0,1.0
18,1002,4,2020-11-02,0,,,0,DEF02,246601.0,1.0
19,1002,4,2020-11-03,0,,,0,DEF02,246601.0,1.0
20,1002,4,2020-11-04,0,,,0,DEF02,246601.0,1.0
21,1002,4,2020-11-05,0,1.0,0.40551334619522095,0,DEF02,246601.0,1.0
22,1002,4,2020-11-06,0,0.0,0.0,0,DEF02,246601.0,1.0
23,1002,4,2020-11-07,0,0.0,0.0,0,DEF02,246601.0,1.0
24,1002,4,2020-11-08,0,0.0,0.0,0,DEF02,246601.0,1.0
25,1002,4,2020-11-09,0,0.0,0.0,0,DEF02,246601.0,1.0
26,1002,4,2020-11-10,0,0.0,0.0,0,DEF02,246601.0,1.0
27,1002,4,2020-11-11,0,0.0,0.0,0,DEF02,246601.0,1.0
28,1002,4,2020-11-12,0,0.0,0.0,0,DEF02,246601.0,1.0
29,1002,4,2020-11-13,0,0.0,0.0,0,DEF02,246601.0,1.0
30,11001,4,2020-10-30,0,,,0,None,,11.0
31,11001,4,2020-10-31,0,,,0,None,,11.0
32,11001,4,2020-11-01,0,,,0,None,,11.0
33,11001,4,2020-11-02,1,,,0,None,,11.0
34,11001,4,2020-11-03,1,,,0,None,,11.0
35,11001,4,2020-11-04,0,,,0,None,,11.0
36,11001,4,2020-11-05,1,3.0,3.3357796669006348,0,None,,11.0
37,11001,4,2020-11-06,0,3.0,3.3357796669006348,0,None,,11.0
38,11001,4,2020-11-07,0,3.0,3.3357796669006348,0,None,,11.0
39,11001,4,2020-11-08,0,3.0,3.3357796669006348,0,None,,11.0
40,11001,4,2020-11-09,0,2.0,2.22385311126709,0,None,,11.0
41,11001,4,2020-11-10,0,1.0,1.111926555633545,0,None,,11.0
42,11001,4,2020-11-11,0,1.0,1.111926555633545,0,None,,11.0
43,11001,4,2020-11-12,0,0.0,0.0,0,None,,11.0
44,11001,4,2020-11-13,0,0.0,0.0,0,None,,11.0
45,11002,4,2020-10-30,0,,,0,None,,11.0
46,11002,4,2020-10-31,0,,,0,None,,11.0
47,11002,4,2020-11-01,0,,,0,None,,11.0
48,11002,4,2020-11-02,0,,,0,None,,11.0
49,11002,4,2020-11-03,0,,,0,None,,11.0
50,11002,4,2020-11-04,0,,,0,None,,11.0
51,11002,4,2020-11-05,0,0.0,0.0,0,None,,11.0
52,11002,4,2020-11-06,0,0.0,0.0,0,None,,11.0
53,11002,4,2020-11-07,0,0.0,0.0,0,None,,11.0
54,11002,4,2020-11-08,1,1.0,1.111926555633545,0,None,,11.0
55,11002,4,2020-11-09,0,1.0,1.111926555633545,0,None,,11.0
56,11002,4,2020-11-10,0,1.0,1.111926555633545,0,None,,11.0
57,11002,4,2020-11-11,0,1.0,1.111926555633545,0,None,,11.0
58,11002,4,2020-11-12,0,1.0,1.111926555633545,0,None,,11.0
59,11002,4,2020-11-13,2,3.0,3.3357796669006348,0,None,,11.0
60,1001,3,2020-10-30,1,,,0,DEF01,89934.0,1.0
61,1001,3,2020-10-31,3,,,0,DEF01,89934.0,1.0
62,1001,3,2020-11-01,0,,,0,DEF01,89934.0,1.0
63,1001,3,2020-11-02,0,,,0,DEF01,89934.0,1.0
64,1001,3,2020-11-03,0,,,0,DEF01,89934.0,1.0
65,1001,3,2020-11-04,0,,,0,DEF01,89934.0,1.0
66,1001,3,2020-11-05,0,4.0,4.44770622253418,0,DEF01,89934.0,1.0
67,1001,3,2020-11-06,0,3.0,3.3357796669006348,0,DEF01,89934.0,1.0
68,1001,3,2020-11-07,0,0.0,0.0,0,DEF01,89934.0,1.0
69,1001,3,2020-11-08,0,0.0,0.0,0,DEF01,89934.0,1.0
70,1001,3,2020-11-09,0,0.0,0.0,0,DEF01,89934.0,1.0
71,1001,3,2020-11-10,0,0.0,0.0,0,DEF01,89934.0,1.0
72,1001,3,2020-11-11,0,0.0,0.0,0,DEF01,89934.0,1.0
73,1001,3,2020-11-12,0,0.0,0.0,0,DEF01,89934.0,1.0
74,1001,3,2020-11-13,0,0.0,0.0,0,DEF01,89934.0,1.0
75,1002,3,2020-10-30,1,,,0,DEF02,246601.0,1.0
76,1002,3,2020-10-31,0,,,0,DEF02,246601.0,1.0
77,1002,3,2020-11-01,0,,,0,DEF02,246601.0,1.0
78,1002,3,2020-11-02,0,,,0,DEF02,246601.0,1.0
79,1002,3,2020-11-03,0,,,0,DEF02,246601.0,1.0
80,1002,3,2020-11-04,0,,,0,DEF02,246601.0,1.0
81,1002,3,2020-11-05,0,1.0,0.40551334619522095,0,DEF02,246601.0,1.0
82,1002,3,2020-11-06,0,0.0,0.0,0,DEF02,246601.0,1.0
83,1002,3,2020-11-07,0,0.0,0.0,0,DEF02,246601.0,1.0
84,1002,3,2020-11-08,0,0.0,0.0,0,DEF02,246601.0,1.0
85,1002,3,2020-11-09,0,0.0,0.0,0,DEF02,246601.0,1.0
86,1002,3,2020-11-10,0,0.0,0.0,0,DEF02,246601.0,1.0
87,1002,3,2020-11-11,0,0.0,0.0,0,DEF02,246601.0,1.0
88,1002,3,2020-11-12,0,0.0,0.0,0,DEF02,246601.0,1.0
89,1002,3,2020-11-13,0,0.0,0.0,0,DEF02,246601.0,1.0
90,11000,3,2020-10-30,0,,,0,DE300,,11.0
91,11000,3,2020-10-31,0,,,0,DE300,,11.0
92,11000,3,2020-11-01,0,,,0,DE300,,11.0
93,11000,3,2020-11-02,1,,,0,DE300,,11.0
94,11000,3,2020-11-03,1,,,0,DE300,,11.0
95,11000,3,2020-11-04,0,,,0,DE300,,11.0
96,11000,3,2020-11-05,1,3.0,inf,0,DE300,,11.0
97,11000,3,2020-11-06,0,3.0,inf,0,DE300,,11.0
98,11000,3,2020-11-07,0,3.0,inf,0,DE300,,11.0
99,11000,3,2020-11-08,1,4.0,inf,0,DE300,,11.0
100,11000,3,2020-11-09,0,3.0,inf,0,DE300,,11.0
101,11000,3,2020-11-10,0,2.0,inf,0,DE300,,11.0
102,11000,3,2020-11-11,0,2.0,inf,0,DE300,,11.0
103,11000,3,2020-11-12,0,1.0,inf,0,DE300,,11.0
104,11000,3,2020-11-13,2,3.0,inf,0,DE300,,11.0
105,1,1,2020-10-30,2,,,0,None,336535.0,1.0
106,1,1,2020-10-31,3,,,0,None,336535.0,1.0
107,1,1,2020-11-01,0,,,0,None,336535.0,1.0
108,1,1,2020-11-02,0,,,0,None,336535.0,1.0
109,1,1,2020-11-03,0,,,0,None,336535.0,1.0
110,1,1,2020-11-04,0,,,0,None,336535.0,1.0
111,1,1,2020-11-05,0,5.0,1.4857295751571655,0,None,336535.0,1.0
112,1,1,2020-11-06,0,3.0,0.8914377689361572,0,None,336535.0,1.0
113,1,1,2020-11-07,0,,,0,None,336535.0,1.0
114,1,1,2020-11-08,0,,,0,None,336535.0,1.0
115,1,1,2020-11-09,0,,,0,None,336535.0,1.0
116,1,1,2020-11-10,0,,,0,None,336535.0,1.0
117,1,1,2020-11-11,0,,,0,None,336535.0,1.0
118,1,1,2020-11-12,0,,,0,None,336535.0,1.0
119,1,1,2020-11-13,0,,,0,None,336535.0,1.0
120,11,1,2020-10-30,0,,,0,None,,11.0
121,11,1,2020-10-31,0,,,0,None,,11.0
122,11,1,2020-11-01,0,,,0,None,,11.0
123,11,1,2020-11-02,1,,,0,None,,11.0
124,11,1,2020-11-03,1,,,0,None,,11.0
125,11,1,2020-11-04,0,,,0,None,,11.0
126,11,1,2020-11-05,1,3.0,inf,0,None,,11.0
127,11,1,2020-11-06,0,3.0,inf,0,None,,11.0
128,11,1,2020-11-07,0,3.0,inf,0,None,,11.0
129,11,1,2020-11-08,1,4.0,inf,0,None,,11.0
130,11,1,2020-11-09,0,3.0,inf,0,None,,11.0
131,11,1,2020-11-10,0,2.0,inf,0,None,,11.0
132,11,1,2020-11-11,0,2.0,inf,0,None,,11.0
133,11,1,2020-11-12,0,1.0,inf,0,None,,11.0
134,11,1,2020-11-13,2,3.0,inf,0,None,,11.0
135,0,0,2020-10-30,2,,,0,None,336535.0,
136,0,0,2020-10-31,3,,,0,None,336535.0,
137,0,0,2020-11-01,0,,,0,None,336535.0,
138,0,0,2020-11-02,1,,,0,None,336535.0,
139,0,0,2020-11-03,1,,,0,None,336535.0,
140,0,0,2020-11-04,0,,,0,None,336535.0,
141,0,0,2020-11-05,1,8.0,2.377167224884033,0,None,336535.0,
142,0,0,2020-11-06,0,6.0,1.7828755378723145,0,None,336535.0,
143,0,0,2020-11-07,0,3.0,0.8914377689361572,0,None,336535.0,
144,0,0,2020-11-08,1,4.0,1.1885836124420166,0,None,336535.0,
145,0,0,2020-11-09,0,3.0,0.8914377689361572,0,None,336535.0,
146,0,0,2020-11-10,0,2.0,0.5942918062210083,0,None,336535.0,
147,0,0,2020-11-11,0,2.0,0.5942918062210083,0,None,336535.0,
148,0,0,2020-11-12,0,1.0,0.29714590311050415,0,None,336535.0,
149,0,0,2020-11-13,2,3.0,0.8914377689361572,0,None,336535.0,
"""
