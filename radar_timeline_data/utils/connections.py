from typing import List

import polars as pl
import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.nhsbt_models as nhsbt
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import String, cast, or_
from sqlalchemy import create_engine, update, Table, MetaData, select
from sqlalchemy.orm import Session


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
        query,
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
            "update_date": pl.Datetime,
        },
    )


def create_sessions() -> dict[str, Session]:
    """

    Returns:
        dict: A dictionary containing initialized SessionManager instances for each database session.
    """

    engine = create_engine(
        "mssql+pyodbc://rr-sql-live/renalreg?driver=SQL+Server+Native+Client+11.0",
        pool_timeout=360000,
    )

    return {
        "ukrdc": PostgresConnection(
            app="ukrdc_staging", tunnel=True, via_app=True
        ).session(),
        "radar": PostgresConnection(
            app="radar_staging", tunnel=True, via_app=True
        ).session(),
        "rr": Session(engine, future=True),
    }


def map_ukrdcid_to_radar_number(sessions: dict[str, Session]) -> pl.DataFrame:
    ukrdc_query = select(
        ukrdc.PatientRecord.pid,
        ukrdc.PatientRecord.ukrdcid,
        ukrdc.PatientRecord.localpatientid,
    ).join(ukrdc.Treatment, ukrdc.Treatment.pid == ukrdc.PatientRecord.pid)

    ukrdc_patient_data = get_data_as_df(sessions["ukrdc"], ukrdc_query)

    # Query to get patient numbers from radar
    # TODO check what sourcetype means (RADAR AND UKRDC)
    radar_query = select(
        radar.PatientNumber.patient_id,
        radar.PatientNumber.number,
    )

    radar_patient_numbers = get_data_as_df(sessions["radar"], radar_query)

    # Merge the DataFrames
    return radar_patient_numbers.join(
        ukrdc_patient_data, left_on="number", right_on="localpatientid", how="inner"
    ).unique(subset=["pid"], keep="first")


def filter_and_convert(df: pl.DataFrame, number_group_id: int) -> List[int]:
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
    return filtered_df.get_column("number").to_list()


def get_rr_to_radarnumber_map(sessions: dict[str, Session]) -> pl.DataFrame:
    """
    This function is designed to map UKKRR numbers to radar numbers by querying data from two different databases (
    radar and rrr) and performing several operations to filter and join the data.
    :param sessions: dict[str, SessionManager] containing rr and radar sessions
    :return: pl.DataFrame containing radar number group and rr number
    """
    q = select(
        radar.PatientNumber.patient_id,
        radar.PatientNumber.number_group_id,
        radar.PatientNumber.number,
    ).where(radar.PatientNumber.number_group_id.in_([120, 121, 122, 124]))

    df = get_data_as_df(sessions["radar"], q).unique()
    nhs_no_filter = filter_and_convert(df, 120)
    chi_no_filter = filter_and_convert(df, 121)
    hsc_filter = filter_and_convert(df, 122)

    q = select(
        nhsbt.UKTPatient.rr_no,
        nhsbt.UKTPatient.new_nhs_no,
        nhsbt.UKTPatient.chi_no,
        nhsbt.UKTPatient.hsc_no,
    ).filter(
        nhsbt.UKTPatient.new_nhs_no.in_(nhs_no_filter)
        | nhsbt.UKTPatient.chi_no.in_(chi_no_filter)
        | nhsbt.UKTPatient.hsc_no.in_(hsc_filter)
    )

    rr_df = get_data_as_df(sessions["rr"], q)
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
    sessions: dict[str, Session], filter: pl.Series
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

    radar_query = select(radar.Dialysi, cast(radar.Dialysi.id, String).label("id_str"))

    df_collection["radar"] = get_data_as_df(sessions["radar"], radar_query)
    # workaround for object type causing weird issues in schema
    df_collection["radar"] = df_collection["radar"].drop("id").rename({"id_str": "id"})
    # TODO filter out ids in radar that have not imported from ukrdc to avoid storing them along script life
    # =================<  GET UKRDC  >===============
    temp = filter.cast(pl.String).to_list()
    # Extract data for "ukrdc" session
    ukrdc_query = (
        sessions["ukrdc"]
        .query(
            ukrdc.Treatment.id,
            ukrdc.Treatment.pid,
            ukrdc.Treatment.idx,
            ukrdc.Treatment.fromtime,
            ukrdc.Treatment.totime,
            ukrdc.Treatment.creation_date,
            ukrdc.Treatment.admitreasoncode,
            ukrdc.Treatment.healthcarefacilitycode,
            ukrdc.PatientRecord.localpatientid,
            ukrdc.PatientRecord.ukrdcid,
            ukrdc.Treatment.update_date,
        )
        .join(ukrdc.PatientRecord, ukrdc.Treatment.pid == ukrdc.PatientRecord.pid)
        .filter(ukrdc.PatientRecord.localpatientid.in_(temp))
        .statement
    )

    df_collection["ukrdc"] = get_data_as_df(sessions["ukrdc"], ukrdc_query)

    return df_collection


def sessions_to_transplant_dfs(
    sessions: dict[str:Session], rr_filter: pl.Series
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding transplants.

    Args:
        sessions (dict): A dictionary containing session information.
        rr_filter (pl.Series):A filter of ids to pull
    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.

    """

    # Initialize dictionary to store DataFrames
    df_collection = {}

    # Extract data for "radar" session

    radar_query = select(
        radar.Transplant.id.label("id_str"),
        radar.Transplant,
    )
    df_collection["radar"] = (
        get_data_as_df(sessions["radar"], radar_query)
        .drop(columns="id")
        .rename({"id_str": "id"})
    )
    temp = rr_filter.to_list()
    in_clause = ",".join([f"'{str(value)}'" for value in temp])

    # transplant unit -> transplant group id
    # will need to add hla 000 column to db

    rr_query = (
        sessions["rr"]
        .query(
            nhsbt.UKTTransplant.rr_no,
            nhsbt.UKTTransplant.transplant_type,
            nhsbt.UKTTransplant.transplant_organ,
            nhsbt.UKTTransplant.transplant_date,
            nhsbt.UKTTransplant.ukt_fail_date,
            nhsbt.UKTTransplant.hla_mismatch,
            nhsbt.UKTTransplant.transplant_relationship,
            nhsbt.UKTTransplant.transplant_sex,
            nhsbt.UKTSites.rr_code.label("TRANSPLANT_UNIT"),
        )
        .join(
            nhsbt.UKTSites,
            nhsbt.UKTTransplant.transplant_unit == nhsbt.UKTSites.site_name,
        )
        .filter(nhsbt.UKTTransplant.rr_no.in_(in_clause))
        .statement
    )

    df_collection["rr"] = get_data_as_df(sessions["rr"], rr_query)
    return df_collection


def get_modality_codes(sessions: dict[str, Session]) -> pl.DataFrame:
    query = select(
        ukrdc.ModalityCodes.registry_code, ukrdc.ModalityCodes.equiv_modality
    )
    return get_data_as_df(sessions["ukrdc"], query).drop_nulls()


def get_sattelite_map(session: Session) -> pl.DataFrame:
    """
    Retrieves satellite mapping data from the database using the provided SessionManager object.
    The data includes satellite codes and their corresponding main unit codes.
    Args:
    - session (SessionManager): The SessionManager object used to interact with the database.

    Returns:
    - pl.DataFrame: A Polars DataFrame containing unique satellite codes and their corresponding main unit codes.
    """
    query = select(ukrdc.SatelliteMap.satellite_code, ukrdc.SatelliteMap.main_unit_code)
    return get_data_as_df(session, query).unique(
        subset=["satellite_code"], keep="first"
    )


def get_source_group_id_mapping(session: Session) -> pl.DataFrame:
    query = select(radar.Group.id, radar.Group.code)
    return get_data_as_df(session, query)


def manual_export_sql(session: Session, data: pl.DataFrame, tablename: str):
    data = data.to_dicts()
    # Reflect the table from the database
    table = Table(tablename, MetaData(), autoload_with=session.bind)
    session.execute(update(table), data)


def export_to_sql(
    session: Session, data: pl.DataFrame, tablename: str, contains_pk: bool
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
