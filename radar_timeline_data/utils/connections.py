import polars as pl
import radar_models.radar2 as radar
import sqlalchemy
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.nhsbt_models as nhsbt
from rr_connection_manager import SQLServerConnection
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import String, cast, FromClause
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(
        (sqlalchemy.exc.TimeoutError, sqlalchemy.exc.OperationalError)
    ),
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

    # =======================<  GET RADAR   >====================

    radar_query = select(radar.Dialysi, cast(radar.Dialysi.id, String).label("id_str"))

    df_collection = {"radar": get_data_as_df(sessions["radar"], radar_query)}
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
        .filter(ukrdc.PatientRecord.ukrdcid.in_(temp))
        .statement
    )

    df_collection["ukrdc"] = get_data_as_df(sessions["ukrdc"], ukrdc_query)

    return df_collection


def sessions_to_transplant_dfs(
    sessions: dict[str, Session], rr_filter: pl.Series
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding transplants.

    Args:
        sessions (dict): A dictionary containing session information.
        rr_filter (pl.Series):A filter of ids to pull
    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.

    """

    # Extract data for "radar" session

    radar_query = select(
        cast(radar.Transplant.id, String).label("id_str"),
        radar.Transplant,
    )
    df_collection = {
        "radar": get_data_as_df(sessions["radar"], radar_query)
        .drop(columns="id")
        .rename({"id_str": "id"})
    }
    in_clause = rr_filter.to_list()

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
    """
    Retrieve modality codes and their equivalent modalities.

    Args:
        sessions (dict): Dictionary of database sessions.

    Returns:
        DataFrame: Modality codes and their equivalent modalities with null values dropped.
    """

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
    """
    Get the mapping of source group IDs to their corresponding codes.

    Args:
        session: Database session.

    Returns:
        DataFrame: Mapping of source group IDs to their codes.
    """

    query = select(radar.Group.id, radar.Group.code)
    return get_data_as_df(session, query)


def df_batch_insert_to_sql(
    dataframe: pl.DataFrame,
    session: Session,
    table: FromClause,
    batch_size: int,
    primary_key: str,
):
    """
    Upsert a DataFrame into a specified SQLAlchemy table.

    Parameters:
    dataframe (pl.DataFrame): The DataFrame to upsert.
    session (sqlalchemy.orm.Session): The SQLAlchemy session to use for the operation.
    table (sqlalchemy.Table.__table__): ?.

    Returns:
    rows_total, rows_failed (int, list[dict[str, Any]])
    """
    # Convert the DataFrame to a list of dictionaries
    rows_total = 0
    rows_failed = []
    for start in range(0, len(dataframe), batch_size):
        end = start + batch_size
        batch = dataframe.slice(start, end)
        batch_null = batch.filter(pl.col(primary_key).is_null()).drop([primary_key])
        batch_id = batch.filter(pl.col(primary_key).is_not_null())
        print(batch_null.to_dicts() + batch_id.to_dicts())
        data = batch_null.to_dicts() + batch_id.to_dicts()
        print(data)
        try:
            # Create an insert statement
            stmt = insert(table).values(data)  # type : ignore

            # Define the update action on conflict
            stmt = stmt.on_conflict_do_update(
                index_elements=[primary_key],  # Specify the primary key column(s)
                set_={
                    col: stmt.excluded[col] for col in data[0].keys()
                },  # Update all columns
            )
        except SQLAlchemyError as e:
            rows_failed.extend(data)

        # Execute the statement and commit the transaction
        result = session.execute(stmt)
        rows_total += result.rowcount
        session.commit()

    return rows_total, rows_failed
