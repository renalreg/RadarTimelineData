import polars as pl
from rr_connection_manager import SQLServerConnection
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import text, update, Table, MetaData


def get_data_as_df(session, query) -> pl.DataFrame:
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
        connection=session.bind,
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
        },
    )


def create_sessions() -> (
    dict[str, PostgresConnection.session | SQLServerConnection.session]
):
    """

    Returns:
        dict: A dictionary containing initialized SessionManager instances for each database session.
    """

    return {
        "ukrdc": PostgresConnection(
            app="ukrdc_staging", tunnel=True, via_app=True
        ).session(),
        "radar": PostgresConnection(
            app="radar_staging", tunnel=True, via_app=True
        ).session(),
        "rr": SQLServerConnection(app="renalreg_live").session(),
    }


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


def filter_and_convert(df: pl.DataFrame, number_group_id: int) -> str:
    """
    converts df with number_group_id and number column to str of integers.
    Args:
        df:
        number_group_id:

    Returns:

    """
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
    result_df = result_df.unique(["RR_NO", "patient_id"])
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

    # =======================<  GET RADAR   >====================

    radar_query = sessions["radar"].session.query(
        text("*, CAST(id AS VARCHAR) AS id_str FROM dialysis")
    )
    df_collection["radar"] = sessions["radar"].get_data_as_df(radar_query)
    # workaround for object type causing weird issues in schema
    df_collection["radar"] = df_collection["radar"].drop("id").rename({"id_str": "id"})
    # TODO filter out ids in radar that have not imported from ukrdc to avoid storing them along script life
    # =================<  GET UKRDC  >===============

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
        ukrdc_filter (pl.Series):A filter of ids to pull
        rr_filter (pl.Series):A filter of ids to pull
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
    temp = rr_filter.to_list()
    in_clause = ",".join([f"'{str(value)}'" for value in temp])

    # transplant unit -> transplant group id
    # will need to add hla 000 column to db

    rr_query = (
        sessions["rr"]
        .session.query(
            text(
                """
    u.[RR_NO],
    u.[TRANSPLANT_TYPE],
    u.[TRANSPLANT_ORGAN],
    u.[TRANSPLANT_DATE],
    u.[UKT_FAIL_DATE],
    u.[HLA_MISMATCH],
    u.[TRANSPLANT_RELATIONSHIP],
    u.[TRANSPLANT_SEX],
    x.[RR_CODE] as TRANSPLANT_UNIT
FROM 
    [renalreg].[dbo].[UKT_TRANSPLANTS] u
LEFT JOIN 
    [renalreg].[dbo].[UKT_SITES] x ON u.[TRANSPLANT_UNIT] = x.[SITE_NAME]"""
            )
        )
        .filter(text(f"[RR_NO] in ({in_clause})"))
    )
    """['patient_id', 'source_group_id', 'source_type', 'transplant_group_id', 'date', 'modality', 'date_of_recurrence', 'date_of_failure', 'recurrence']"""

    df_collection["rr"] = sessions["rr"].get_data_as_df(rr_query)
    return df_collection


def get_modality_codes(sessions: dict[str, SessionManager]) -> pl.DataFrame:
    query = sessions["ukrdc"].session.query(
        text("""registry_code, equiv_modality FROM modality_codes""")
    )
    codes = sessions["ukrdc"].get_data_as_df(query).drop_nulls()
    return codes


def get_sattelite_map(session: SessionManager) -> pl.DataFrame:
    """
    Retrieves satellite mapping data from the database using the provided SessionManager object.
    The data includes satellite codes and their corresponding main unit codes.
    Args:
    - session (SessionManager): The SessionManager object used to interact with the database.

    Returns:
    - pl.DataFrame: A Polars DataFrame containing unique satellite codes and their corresponding main unit codes.
    """
    query = session.session.query(
        text(""" satellite_code, main_unit_code FROM satellite_map""")
    )
    return session.get_data_as_df(query).unique(subset=["satellite_code"], keep="first")


def get_source_group_id_mapping(session: SessionManager) -> pl.DataFrame:
    query = session.session.query(
        text(""" id, code FROM public.groups ORDER BY id ASC """)
    )
    return session.get_data_as_df(query)


def manual_export_sql(session: SessionManager, data: pl.DataFrame, tablename: str):
    session = session.session
    data = data.to_dicts()
    # Reflect the table from the database
    table = Table(tablename, MetaData(), autoload_with=session.bind)
    session.execute(update(table), data)


def export_to_sql(
    session: SessionManager, data: pl.DataFrame, tablename: str, contains_pk: bool
) -> None:
    if contains_pk:
        manual_export_sql(session, data, tablename)
    else:
        data.write_database(
            table_name=tablename,
            connection=session.bind.url,
            if_table_exists="append",
            engine="sqlalchemy",
        )
