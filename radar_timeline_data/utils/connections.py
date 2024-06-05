from typing import List

import polars as pl
import radar_models.radar2 as radar
import sqlalchemy
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.nhsbt_models as nhsbt
from rr_connection_manager import SQLServerConnection
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import String, cast
from sqlalchemy import create_engine, update, Table, MetaData, select
from sqlalchemy.orm import Session
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(sqlalchemy.exc.TimeoutError),
)
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
            "chi_no": pl.String,
            "hsc_no": pl.String,
            "update_date": pl.Datetime,
            "new_nhs_no": pl.String,
            "radar_id": pl.String,
            "rr_no": pl.String,
        },
    )


def create_sessions() -> dict[str, Session]:
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
    filtered_df = (
        df.filter(pl.col("number_group_id") == number_group_id)
        .cast({"number": pl.Int64})
        .unique(subset=["number"], keep="first")
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
    rr_df = pl.DataFrame()
    q = select(
        nhsbt.UKTPatient.rr_no,
        nhsbt.UKTPatient.new_nhs_no,
        nhsbt.UKTPatient.chi_no,
        nhsbt.UKTPatient.hsc_no,
    )
    rr_df = get_database_with_multiple_filters(
        [nhs_no_filter, chi_no_filter, hsc_filter],
        [nhsbt.UKTPatient.new_nhs_no, nhsbt.UKTPatient.chi_no, nhsbt.UKTPatient.hsc_no],
        rr_df,
        sessions["rr"],
        q,
    )

    a = rr_df.filter(pl.col("new_nhs_no").is_not_null()).cast({"new_nhs_no": pl.String})
    b = rr_df.filter(pl.col("chi_no").is_not_null()).cast({"chi_no": pl.String})
    c = rr_df.filter(pl.col("hsc_no").is_not_null()).cast({"hsc_no": pl.String})
    nhs_df = df.filter(pl.col("number_group_id") == 120).join(
        a.select(["new_nhs_no", "rr_no"]), left_on="number", right_on="new_nhs_no"
    )
    chi_df = df.filter(pl.col("number_group_id") == 121).join(
        b.select(["chi_no", "rr_no"]), left_on="number", right_on="chi_no"
    )
    hsc_df = df.filter(pl.col("number_group_id") == 122).join(
        c.select(["hsc_no", "rr_no"]), left_on="number", right_on="hsc_no"
    )
    result_df = pl.concat([nhs_df, chi_df, hsc_df])
    result_df = result_df.unique(["rr_no", "patient_id"])
    result_df = result_df.drop("number").rename({"rr_no": "number"}).sort("patient_id")
    # TODO below statemnet is a temp fix as some numbers are null which should not be the case
    result_df = result_df.filter(pl.col("number").is_not_null())
    return result_df


def get_database_with_multiple_filters(
    no_filters, filter_names, rr_df, session, original_query
):
    """
    data from the database based on multiple filters.

    Args:
        no_filters (list): List of filter values.
        filter_names (list): List of filter names.
        rr_df (DataFrame): Dataframe to store the filtered data.
        session: Database session.
        original_query: Original query to filter data.

    Returns:
        DataFrame: Dataframe with filtered data.
    """

    chunk_size = 2000  # Adjust based on your needs
    for no_filter, filter_name in zip(no_filters, filter_names):
        chunks = [
            no_filter[i : i + chunk_size] for i in range(0, len(no_filter), chunk_size)
        ]
        for chunk in chunks:
            query = original_query.filter(filter_name.in_(chunk))
            if rr_df.is_empty():
                rr_df = get_data_as_df(session, query)
            else:
                rr_df = pl.concat([rr_df, get_data_as_df(session, query)])
    # TODO CHECK THAT CONCAT HAS ALL VALUES AND IS NOT MISSING ANY
    return rr_df


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
        cast(radar.Transplant.id, String).label("id_str"),
        radar.Transplant,
    )
    df_collection["radar"] = (
        get_data_as_df(sessions["radar"], radar_query)
        .drop(columns="id")
        .rename({"id_str": "id"})
    )
    in_clause = rr_filter.to_list()

    # transplant unit -> transplant group id
    # will need to add hla 000 column to db

    rr_query = select(
        nhsbt.UKTTransplant.rr_no,
        nhsbt.UKTTransplant.transplant_type,
        nhsbt.UKTTransplant.transplant_organ,
        nhsbt.UKTTransplant.transplant_date,
        nhsbt.UKTTransplant.ukt_fail_date,
        nhsbt.UKTTransplant.hla_mismatch,
        nhsbt.UKTTransplant.transplant_relationship,
        nhsbt.UKTTransplant.transplant_sex,
        nhsbt.UKTSites.rr_code.label("TRANSPLANT_UNIT"),
    ).join(
        nhsbt.UKTSites,
        nhsbt.UKTTransplant.transplant_unit == nhsbt.UKTSites.site_name,
    )
    df_collection["rr"] = pl.DataFrame()
    df_collection["rr"] = get_database_with_multiple_filters(
        [in_clause],
        [nhsbt.UKTTransplant.rr_no],
        df_collection["rr"],
        sessions["rr"],
        rr_query,
    )
    # df_collection["rr"] = get_data_as_df(sessions["rr"], rr_query)
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
