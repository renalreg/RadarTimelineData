"""
A class for retrieving data from an SQLAlchemy database. It provides methods to either
return a session object for direct database interaction or retrieve data as a pandas/polars
DataFrame.

Usage Example:
    data_getter = DataGetter("sqlite:///example.db")
    session = data_getter.get_session()
    df = data_getter.get_data_as_df("SELECT * FROM table_name")

Attributes:
    engine (sqlalchemy.engine.Engine): SQLAlchemy Engine object representing the database connection.

Methods:
    get_session(): Returns a SQLAlchemy session object connected to the database.
    get_data_as_df(query): Retrieves data from the database using the provided query and returns it
                           as a pandas DataFrame.

Args:
    db_uri (str): The URI of the database to connect to.

Returns:
    None

"""

import polars as pl
from rr_connection_manager import SQLServerConnection
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


class SessionManager:
    def __init__(
        self,
        db_uri: str | None = None,
        driver: str | None = None,
        connection_manger_passthrough: str | None = None,
    ):
        if connection_manger_passthrough:
            if connection_manger_passthrough == "rr_live":
                conn = SQLServerConnection(app="rr_live")
            else:
                conn = PostgresConnection(
                    app=connection_manger_passthrough, tunnel=True, via_app=True
                )
            self.session = conn.session()
            self.engine = self.session.bind
        else:
            self.engine = create_engine(
                f"{db_uri}?driver={driver}", pool_timeout=360000
            )
            self.session = Session(self.engine, future=True)

    def get_data_as_df(self, query) -> pl.DataFrame:
        """
        Retrieves data from the database using the provided query and returns it as a Polars DataFrame.

        Args:
        - query (str): SQL query to execute

        Returns:
        - Polars DataFrame containing the result of the query
        """
        # TODO convert to database uri
        return pl.read_database(
            query.statement,
            connection=self.session.bind,
            schema_overrides={
                "updatedon": pl.Datetime,
                "externalid": pl.String,
                "donor_hla": pl.String,
                "recipient_hla": pl.String,
                "graft_loss_cause": pl.String,
                "date_of_cmv_infection": pl.Date,
                "date": pl.Date,
                "date_of_failure": pl.Date,
                "date_of_recurrence": pl.Date,
                "CHI_NO": pl.String,
                "HSC_NO": pl.String,
                str: pl.String,
            },
        )

    def get_data_as_result(self, query):
        return self.session.query(query)


def create_sessions() -> dict[str, SessionManager]:
    """
    Create and initialize session managers for different databases.

    Returns:
        dict: A dictionary containing initialized SessionManager instances for each database session.
    """
    sessions = {
        "ukrdc": SessionManager(
            connection_manger_passthrough="ukrdc_staging",
        ),
        "radar": SessionManager(connection_manger_passthrough="radar_staging"),
        "rr": SessionManager(connection_manger_passthrough="rr_live"),
    }
    return sessions


def get_ukrdcid_to_radarnumber_map(sessions: dict[str, SessionManager]) -> pl.DataFrame:
    # TODO check for any refactoring in the query as a join may not be needed will need to look at mapper that uses return value
    ukrdc_query = sessions["ukrdc"].session.query(
        text(
            """
            pr.pid,
            pr.ukrdcid,
            pr.localpatientid
            FROM treatment AS t 
            JOIN patientrecord AS pr
            ON t.pid = pr.pid
            """
        )
    )
    ukrdc_patient_data = sessions["ukrdc"].get_data_as_df(ukrdc_query)

    # Query to get patient numbers from radar
    # TODO check what sourcetype means (RADAR AND UKRDC)
    radar_query = sessions["radar"].session.query(
        text("patient_id, number FROM public.patient_numbers")
    )
    radar_patient_numbers = sessions["radar"].get_data_as_df(radar_query)

    # Merge the DataFrames
    return radar_patient_numbers.join(
        ukrdc_patient_data, left_on="number", right_on="localpatientid", how="inner"
    ).unique(subset=["pid"], keep="first")


def filter_and_convert(df, number_group_id):
    filtered_df = df.filter(pl.col("number_group_id") == number_group_id).cast(
        {"number": pl.Int64}
    )
    return ",".join(
        [f"'{str(value)}'" for value in filtered_df.get_column("number").to_list()]
    )


def get_rr_to_radarnumber_map(sessions: dict[str, SessionManager]) -> pl.DataFrame:
    """
    This function is designed to map UKKRR numbers to radar numbers by querying data from two different databases (
    radar and rrr) and performing several operations to filter and join the data.
    :param sessions: dict[str, SessionManager] containing rr and radar sessions
    :return: pl.DataFrame containing radar number group and rr number
    """
    q = sessions["radar"].session.query(
        text(
            """ patient_id, number_group_id, number FROM public.patient_numbers
    WHERE number_group_id IN (120,121,122,124)"""
        )
    )
    df = sessions["radar"].get_data_as_df(q).unique()
    nhs_no_filter = filter_and_convert(df, 120)
    chi_no_filter = filter_and_convert(df, 121)
    hsc_filter = filter_and_convert(df, 122)

    q = (
        sessions["rr"]
        .session.query(
            text(
                """ [RR_NO],[NEW_NHS_NO],[CHI_NO],[HSC_NO] FROM [renalreg].[dbo].[PATIENTS]"""
            )
        )
        .filter(
            text(
                f"""(
        [NEW_NHS_NO] IN ({nhs_no_filter}) OR
         [CHI_NO] IN ({chi_no_filter}) OR
          [HSC_NO] IN ({hsc_filter})
         )"""
            )
        )
    )
    rr_df = sessions["rr"].get_data_as_df(q)
    a = rr_df.filter(pl.col("NEW_NHS_NO").is_not_null()).cast({"NEW_NHS_NO": pl.String})
    b = rr_df.filter(pl.col("CHI_NO").is_not_null()).cast({"CHI_NO": pl.String})
    c = rr_df.filter(pl.col("HSC_NO").is_not_null()).cast({"HSC_NO": pl.String})
    nhs_df = df.filter(pl.col("number_group_id") == 120).join(
        a.select(["NEW_NHS_NO", "RR_NO"]), left_on="number", right_on="NEW_NHS_NO"
    )
    chi_df = df.filter(pl.col("number_group_id") == 121).join(
        b.select(["CHI_NO", "RR_NO"]), left_on="number", right_on="CHI_NO"
    )
    hsc_df = df.filter(pl.col("number_group_id") == 122).join(
        c.select(["HSC_NO", "RR_NO"]), left_on="number", right_on="HSC_NO"
    )
    result_df = pl.concat([nhs_df, chi_df, hsc_df])
    result_df = result_df.unique()
    result_df = result_df.drop("number").rename({"RR_NO": "number"}).sort("patient_id")
    return result_df


def sessions_to_treatment_dfs(
    sessions: dict, filter: pl.Series
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding treatments.

    Args:
        sessions (dict): A dictionary containing session information.
        filter (pl.Series, optional):A filter of ids to pull

    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.
    """

    # Initialize dictionary to store DataFrames
    df_collection = {}

    # Extract data for "radar" session
    radar_query = sessions["radar"].session.query(
        text("*, CAST(id AS VARCHAR) AS id_str FROM dialysis")
    )
    df_collection["radar"] = sessions["radar"].get_data_as_df(radar_query)

    # TODO clean this up, see if in clause can be improved
    temp = filter.to_list()
    in_clause = ",".join([f"'{str(value)}'" for value in temp])
    # Extract data for "ukrdc" session
    ukrdc_query = (
        sessions["ukrdc"]
        .session.query(
            text(
                """id,
                t.pid,
                t.idx,
                t.fromtime,
                t.totime,
                t.creation_date,
                t.admitreasoncode,
                t.healthcarefacilitycode,
                pr.localpatientid,
                pr.ukrdcid,
                t.update_date FROM treatment as t
                JOIN patientrecord AS pr
                ON t.pid = pr.pid"""
            )
        )
        .filter(text(f"pr.localpatientid IN ({in_clause})"))
    )
    df_collection["ukrdc"] = sessions["ukrdc"].get_data_as_df(ukrdc_query)

    return df_collection


def sessions_to_transplant_dfs(
    sessions: dict, ukrdc_filter: pl.Series, rr_filter: pl.Series
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding transplants.

    Args:
        sessions (dict): A dictionary containing session information.
        ukrdc_filter (pl.Series, optional):A filter of ids to pull
        rr_filter:
    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.

    """

    # Initialize dictionary to store DataFrames
    df_collection = {}

    # Extract data for "radar" session
    radar_query = sessions["radar"].session.query(
        text("CAST(id AS VARCHAR) AS id_str, *" " FROM transplants")
    )
    df_collection["radar"] = (
        sessions["radar"]
        .get_data_as_df(radar_query)
        .drop(columns="id")
        .rename({"id_str": "id"})
    )

    # TODO clean this up, see if in clause can be improved
    temp = ukrdc_filter.to_list()
    in_clause = ",".join([f"'{str(value)}'" for value in temp])
    # Extract data for "ukrdc" session
    ukrdc_query = (
        sessions["ukrdc"]
        .session.query(
            text(
                """
                t.id,
                t.pid,
                t.idx,
                t.proceduretypecode,
                t.proceduretypecodestd,
                t.proceduretypedesc,
                t.cliniciancode,
                t.cliniciancodestd,
                t.cliniciandesc,
                t.proceduretime,
                t.enteredbycode,
                t.enteredbycodestd,
                t.enteredbydesc,
                t.enteredatcode,
                t.enteredatcodestd,
                t.enteredatdesc,
                t.updatedon,
                t.actioncode,
                t.externalid,
                t.creation_date,
                t.update_date,
                pr.localpatientid,
                pr.ukrdcid 
                FROM transplant as t
                JOIN patientrecord AS pr
                ON t.pid = pr.pid"""
            )
        )
        .filter(text(f"pr.localpatientid IN ({in_clause})"))
    )
    df_collection["ukrdc"] = sessions["ukrdc"].get_data_as_df(ukrdc_query)

    temp = rr_filter.to_list()
    in_clause = ",".join([f"'{str(value)}'" for value in temp])

    rr_query = (
        sessions["rr"]
        .session.query(text("""* FROM [renalreg].[dbo].[UKT_TRANSPLANTS] """))
        .filter(text(f"[RR_NO] in ({in_clause})"))
    )
    df_collection["rr"] = sessions["rr"].get_data_as_df(rr_query)
    return df_collection


def get_modality_codes(sessions: dict[str, SessionManager]) -> pl.DataFrame:
    query = sessions["ukrdc"].session.query(
        text("""registry_code, equiv_modality FROM modality_codes""")
    )
    codes = sessions["ukrdc"].get_data_as_df(query).drop_nulls()
    return codes


def get_sattelite_map(session: SessionManager) -> pl.DataFrame:
    query = session.session.query(
        text(""" satellite_code, main_unit_code FROM satellite_map""")
    )
    return session.get_data_as_df(query).unique(subset=["satellite_code"], keep="first")


def get_source_group_id_mapping(session: SessionManager) -> pl.DataFrame:
    query = session.session.query(
        text(""" id, code FROM public.groups ORDER BY id ASC """)
    )
    return session.get_data_as_df(query)
