import pandas as pd
import ramda as R
import numpy as np
from typing import List, TypedDict, Union
from datetime import datetime
from psycopg2.sql import SQL
from src.lib.dag_helpers import (
    execute_query_and_return_dataframe,
    connect_to_db_and_upsert_pandas_dataframe_on_constraint,
)
from src.lib.test_helpers import set_env_variable_from_dag_config_if_present
from postgres_helpers import create_db_context, teardown_db_context, DBContext


class UserVaccinationData(TypedDict):
    user_id: str
    test_week_start: datetime
    administered_vaccine_doses: Union[int, None]
    days_since_last_dose: Union[int, None]


def add_vaccination_data_to_homogenized_feature_table(**kwargs):
    R.pipe(
        set_env_variable_from_dag_config_if_present("TARGET_DB"),
        load_and_transform_vaccination_data_for_each_user,
        connect_to_db_and_upsert_pandas_dataframe_on_constraint(
            "datenspende_derivatives", "homogenized_features", ["unique_answers"]
        ),
    )(kwargs)


def load_and_transform_vaccination_data_for_each_user(
    *_,
):
    """
    Load unified weekly survey data and determine vaccination status:

    * days_since_last_dose: difference between the date of the last vaccine dose and the begin of the week of the survey
    * administered_vaccine_doses:  sum  of vaccine doses administered before the start of  the week  of the survey

    for each entry.
    Add vaccination status to unified featuers table.
    """
    db_context = create_db_context()
    vaccination_data = load_vaccinations(db_context)
    feature_data = load_feature_data(db_context)
    teardown_db_context(db_context)

    return R.pipe(
        R.invoker(0, "itertuples"),
        R.reduce(vaccination_data_reducer(vaccination_data), []),
        pd.DataFrame,
    )(feature_data)


@R.curry
def vaccination_data_reducer(
    vaccination_data: pd.DataFrame,
    accumulator: List[UserVaccinationData],
    feature_data_row: pd.DataFrame,
) -> List[UserVaccinationData]:
    """
    Calculate days_since_last_dose and number of administered_vaccine_doses from the vaccine_data
     for the user_id and date in the feature_data_row and appends a new item of UserVaccinationData to the results list
    """
    try:
        user_vaccination_data = vaccination_data.set_index("user_id").loc[
            [feature_data_row.user_id]
        ]
    except KeyError:
        return accumulator + [
            {
                "user_id": feature_data_row.user_id,
                "test_week_start": feature_data_row.test_week_start,
                "administered_vaccine_doses": None,
                "days_since_last_dose": None,
            }
        ]

    dose_administration_dates = user_vaccination_data[
        ["first_dose", "second_dose", "third_dose"]
    ].values[0]

    past_dose_administration_dates = [
        pd.Timestamp(date)
        for date in dose_administration_dates
        if pd.Timestamp(date) < pd.Timestamp(feature_data_row.test_week_start)
    ]

    number_of_administered_doses = len(past_dose_administration_dates)
    try:
        days_since_last_dose = (
            pd.Timestamp(feature_data_row.test_week_start)
            - max(past_dose_administration_dates)
        ).days
    except ValueError:
        days_since_last_dose = None
    return accumulator + [
        {
            "user_id": feature_data_row.user_id,
            "test_week_start": feature_data_row.test_week_start,
            "administered_vaccine_doses": number_of_administered_doses,
            "days_since_last_dose": days_since_last_dose,
        }
    ]


def load_feature_data(db_context: DBContext) -> pd.DataFrame:
    return execute_query_and_return_dataframe(
        SQL("""SELECT * FROM datenspende_derivatives.homogenized_features"""),
        db_context,
    )


def _convert_date(date):
    """
    Author: @marcwie
    Convert a date string to a datetime object.
    Works for german strings of the format 'MONTH YEAR', e.g., 'Mai 2020' as
    they are used in the database to indicate vaccination dates.
    Parameters:
    -----------
    date : str
        Date string (in german) of the format specified above.
    Returns:
    --------
    datetime object corresponding to the date string.
    """

    translation = {
        "Januar": "January",
        "Februar": "February",
        "März": "March",
        "Mai": "May",
        "Juni": "June",
        "Juli": "July",
        "Oktober": "October",
        "Dezember": "December",
    }

    # Sometimes nan is passed to the function. In that case simply return nan
    # back
    if isinstance(date, float):
        if np.isnan(date):
            return date

    # Split the string and translate to english
    month, year = date.split(" ")
    if month in translation.keys():
        month = translation[month]

    # Parse translated string and return datetime object
    return datetime.strptime(month + " " + year, "%B %Y")


def _vaccination_one_survey(db_context: DBContext, questionnaire: int):
    """
    Author: @marcwie
    Load vaccination data from one of the two corresponding surveys.
    This helper function should not be called directly! Use vaccinations()
    instead.
    Parameters:
    -----------
    questionnaire : int
        The id of the questionnaire (either 10 or 13). 10 corresponds to the
        test & symptons survey. 13 to the vaccination update survey that was
        launched in December 2021.
    Returns:
    --------
    pandas.DataFrame of the following format (with integer index that is
    omitted here):
        user_id   status  first_dose     second_dose     third_dose
            330  booster   März 2021        Mai 2021  Dezember 2021
            457     full   Juli 2021     August 2021            NaN
            615     full  April 2021       Juni 2021            NaN
            ...      ...         ...             ...            ...
        1226668  booster    Mai 2021       Juni 2021  Dezember 2021
        1226678  booster   Juni 2021       Juli 2021  Dezember 2021
        1226696  booster    Mai 2021       Juli 2021  Dezember 2021
    If the parameter 'questionnaire' is 10 all entries in third_dose are NaN
    since that information is only surveyed in questionnaire 13.
    """

    print(f"Loading vaccination data from questionnaire {questionnaire}...")

    query = """
    SELECT
        answers.user_id,
        choice.text,
        answers.question,
        answers.questionnaire_session
    FROM
        datenspende.answers, datenspende.choice
    WHERE
        answers.question IN (121, 122, 130, 134, 136) AND
        answers.created_at > 1634630400000 AND
        answers.element = choice.element AND
        answers.questionnaire = {0}
    """.format(
        questionnaire
    )

    data = pd.read_sql_query(query, db_context["connection"])

    # Create base data frame with the vaccination status of all users.
    # Questions 121 and 134 ask about the status. 121 is only used in
    # questionnaire 10, 134 is used in questionaire 13 and has also replaced
    # 121 in questionnaire 10 after December 2021
    df = data[data.question.isin((121, 134))].drop(columns="question")
    df.rename(columns={"text": "status"}, inplace=True)

    # Create columns for first, second and third dose
    for question_id, label in (
        (122, "first_dose"),
        (130, "second_dose"),
        (136, "third_dose"),
    ):
        dose_info = data[data.question == question_id].drop(columns="question")
        df = pd.merge(
            df, dose_info, on=["user_id", "questionnaire_session"], how="outer"
        )
        df.rename(columns={"text": label}, inplace=True)

    # Due to a bug on thryve's end some users can submit data multiple
    # times. Even worse, they can even respond differently accross sessions.
    # In that case we only keep each last UNIQUE response of each user
    df.sort_values(by=["user_id", "questionnaire_session"], inplace=True)
    df.drop_duplicates(subset="user_id", keep="last", inplace=True)
    df.drop(columns="questionnaire_session", inplace=True)

    # Translate responses
    df.status.replace(
        {
            "Ja": "full",
            "Nein, nur teilweise geimpft": "partial",
            "Nein, überhaupt nicht geimpft": "unvaccinated",
            "mit Auffrischimpfung (Booster)": "booster",
            "vollständig erstimunisiert (zweite Dosis im Fall von Moderna, "
            + "Biontech, Astra Zeneca oder erste Impfdosis im Fall von Johnson&Johnson)": "full",
            "unvollständig erstimunisiert (nur erste Impfdosis im Fall von Moderna, Biontech, Astra Zeneca)": "partial",
            "gar nicht geimpft": "unvaccinated",
        },
        inplace=True,
    )

    # Remove implausible responses
    df = _remove_implausible_responses(df)

    return df.reset_index(drop=True)


def _remove_implausible_responses(df):
    """
    Author: @marcwie
    Remove implausible responses from the vaccination data.
    Implausible responses include:
        - unvaccinated users entering dates of any dose
        - partially vaccinated users entering a second or third dose
        - fully vaccinated (vollstaendig erstimmunisiert) users entering a
        third dose
        - partially vaccinated users missing a first dose
        - fully vaccinated users missing a first or second dose
        - boostered users missing any dose
    For now this also removes a small set of users that is considered boostered
    but did not receive three doses due to a previous infection or other
    reasons.
    Parameters:
    -----------
    df : pandas.DataFrame
        A dataframe of the format specified in _vaccination_one_survey()
    Returns:
    --------
    pandas.DataFrame of the same format as the input but with implausible
    responses removed.
    """

    check = [
        ("unvaccinated", "first_dose"),
        ("unvaccinated", "second_dose"),
        ("unvaccinated", "third_dose"),
        ("partial", "second_dose"),
        ("partial", "third_dose"),
        ("full", "third_dose"),
    ]

    for status, column in check:
        invalid = (df.status == status) & ~df[column].isna()
        if invalid.sum():
            print("Dropping", invalid.sum(), status, "that still provide", column)
            df = df[~invalid]

    check = [
        ("partial", "first_dose"),
        ("full", "first_dose"),
        ("full", "second_dose"),
        ("booster", "first_dose"),
        ("booster", "second_dose"),
        ("booster", "third_dose"),
    ]

    for status, column in check:
        invalid = (df.status == status) & df[column].isna()
        if invalid.sum():
            print("Dropping", invalid.sum(), status, "with missing", column)
            df = df[~invalid]

    # Remove users where status is missing
    invalid = df.status.isna()
    if invalid.sum():
        print("Dropping", invalid.sum(), "users with missing status.")
        df = df[~invalid]

    # TODO: Properly treat users that are considered boostered, but either
    # didn't get a second dose because of infection or because their first dose
    # with Johnson & Johnson
    invalid = (df.status == "booster") & df.second_dose.str.contains("Ich")
    if invalid.sum():
        print(
            "Dropping",
            invalid.sum(),
            "users that are considered boostered despite not receiveing an official second dose.",
        )
        df = df[~invalid]

    print(len(df), "valid entries")

    return df


def _merged_vaccination_data(db_context: DBContext):
    """
    Author: @marcwie
    Compiles merged vaccination information from questionnaire 10 and 13.
    This helper function should not be called directly! Use
    vaccinations(which='all') instead.
    Returns:
    --------
    pandas.DataFrame of the following format (with integer index that is
    omitted here):
           user_id   status    first_dose   second_dose     third_dose
               239     full    April 2021     Juni 2021            NaN
               336     full      Mai 2021     Juli 2021            NaN
               375     full      Mai 2021     Juni 2021            NaN
               401     full     Juni 2021     Juli 2021            NaN
               473     full    April 2021   August 2021            NaN
               ...      ...           ...           ...            ...
           1221451     full      Mai 2021     Juni 2021            NaN
           1221463  booster  Februar 2021  Februar 2021   Oktober 2021
           1221464     full     Juli 2021   August 2021            NaN
           1221475     full      Mai 2021     Juli 2021            NaN
           1221479  booster     Juni 2021     Juli 2021  Dezember 2021
    """
    # Load both surveys
    initial = _vaccination_one_survey(db_context, questionnaire=10)
    update = _vaccination_one_survey(db_context, questionnaire=13)

    print("Merging vaccination tables...")

    # Find users that responsed to both surveys and put all per-user
    # information in one row
    common_users = np.intersect1d(initial.user_id, update.user_id)
    overlap = pd.merge(
        initial[initial.user_id.isin(common_users)],
        update[update.user_id.isin(common_users)],
        on="user_id",
    )

    # Filter out users with inconsistent responses accross surveys. See the
    # print statements for details.
    invalid = overlap.status_x.isin(["full", "partial"]) & (
        overlap.first_dose_x != overlap.first_dose_y
    )
    print(
        "Dropping",
        invalid.sum(),
        "fully or partially vaccinated users with inconsistent response in first dose.",
    )
    overlap = overlap[~invalid]

    invalid = overlap.status_x.isin(["full"]) & (
        overlap.second_dose_x != overlap.second_dose_y
    )
    print(
        "Dropping",
        invalid.sum(),
        "fully vaccinated users with inconsistent response in second dose.",
    )
    overlap = overlap[~invalid]

    invalid = overlap.status_x.isin(["full"]) & overlap.status_y.isin(
        ["partial", "unvaccinated"]
    )
    print(
        "Dropping",
        invalid.sum(),
        "users that went from full to partial vaccination or unvaccinated.",
    )
    overlap = overlap[~invalid]

    invalid = overlap.status_x.isin(["partial"]) & overlap.status_y.isin(
        ["unvaccinated"]
    )
    print(
        "Dropping",
        invalid.sum(),
        "users that went from partial vaccination to unvaccinated.",
    )
    overlap = overlap[~invalid]

    # Once users are properly filtered, only keep information from survey 13
    # since that is the most recent one
    overlap.columns = overlap.columns.str.strip("_y")
    overlap.drop(
        columns=["status_x", "first_dose_x", "second_dose_x", "third_dose_x"],
        inplace=True,
    )

    # Put together unique users in questionnaire 10 and 13 as well as the
    # cleaned up overlap
    final = pd.concat(
        [
            initial[~initial.user_id.isin(common_users)],
            update[~update.user_id.isin(common_users)],
            overlap,
        ]
    )

    return final


def load_vaccinations(db_context: DBContext):
    """
    Author: @marcwie
    Get vaccination data from the ROCS database.
    Vaccination data is surveyed in two questionnaires, i.e., questionnaire 10
    (the tests & symptoms study) or questionnaire 13 (the vaccination update
    survey from December 2021).
    Allows parsing only either of the two studies or a consistently merged
    version of both.
    Parameters:
    -----------
    which : str
        Can be either of three options:
            - 'all': Load the data from both surveys (10 and 13)
            - 'initial': Loads only questionnaire 10
            - 'update': Loads only questionnaire 13
    Returns:
    --------
    pandas.DataFrame of the following format (with integer index that is
    omitted here):
       user_id   status first_dose second_dose third_dose jansen_received previously_infected
           239     full 2021-04-01  2021-06-01        NaT           False               False
           336     full 2021-05-01  2021-07-01        NaT           False               False
           375     full 2021-05-01  2021-06-01        NaT           False               False
           387     full 2021-03-01  2021-12-01        NaT           False               False
           401     full 2021-06-01  2021-07-01        NaT           False               False
           ...      ...        ...         ...        ...             ...                 ...
       1226663  booster 2021-06-01  2021-07-01 2021-12-01           False               False
       1226668  booster 2021-05-01  2021-06-01 2021-12-01           False               False
       1226678  booster 2021-06-01  2021-07-01 2021-12-01           False               False
       1226696  booster 2021-05-01  2021-07-01 2021-12-01           False               False
       1226708     full 2021-09-01         NaT        NaT            True               False
    status can be either of four options:
        - 'full', 'booster', 'unvaccinated' or 'partial'
    jansen_received indicates whether users received vaccination from Johnson &
    Johnson. In that case their status reads 'full' despite second_dose being
    NaT.
    previously_infected indicates whether users received only one dose for full
    vaccination because of 'other reasons' which we assume to mostly be
    previous infections. In that case their status reads 'full' despite
    second_dose being NaT.
    """
    df = _merged_vaccination_data(db_context)

    # Create columns for Johnson & Johnson as well as previous infections
    df["jansen_received"] = df.second_dose.str.contains("Johnson")
    df["previously_infected"] = df.second_dose.str.contains("anderen")

    # Replace responses in second_dose with nan-values
    df.second_dose.replace(
        {
            "Ich wurde mit dem Vakzin von Johnson & Johnson "
            + "geimpft und benötigte daher keine zweite Impfdosis.": np.nan,
            "Ich habe aus anderen Gründen keine zweite Dosis erhalten": np.nan,
        },
        inplace=True,
    )

    # Convert datestrings to datetime
    df.first_dose = df.first_dose.apply(_convert_date)
    df.second_dose = df.second_dose.apply(_convert_date)
    df.third_dose = df.third_dose.apply(_convert_date)

    # Remove users where the order of doses is incorrect
    for col1, col2 in (
        ("first_dose", "second_dose"),
        ("first_dose", "third_dose"),
        ("second_dose", "third_dose"),
    ):
        invalid = ~df[col1].isna() & ~df[col2].isna() & (df[col1] > df[col2])
        print("Dropping", invalid.sum(), "users where", col1, "is larger than", col2)
        df = df[~invalid]

    # Sort for better readibility
    df.sort_values(by="user_id", inplace=True)

    return df.reset_index(drop=True)
