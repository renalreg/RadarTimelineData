import polars as pl
import radar_models.radar2 as radar
import sqlalchemy
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.nhsbt_models as nhsbt
from rr_connection_manager import SQLServerConnection
from rr_connection_manager.classes.postgres_connection import PostgresConnection
from sqlalchemy import FromClause, String, cast, select, text, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from ukrdc_sqla.ukrdc import column_names

from radar_timeline_data.utils.config import override_dict
from radar_timeline_data.utils.utils import sqla_to_polars_schema


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(sqlalchemy.exc.TimeoutError),
)
def get_data_as_df(session, query, model=None) -> pl.DataFrame:
    """
    Retrieves data from the database using the provided query and returns it as a Polars DataFrame.

    Args:
    - query (str): SQL query to execute

    Returns:
    - Polars DataFrame containing the result of the query
    """
    # TODO convert to database uri
    schema = override_dict
    if model is not None:
        schema = sqla_to_polars_schema(model)

    return pl.read_database(
        query,
        connection=session.bind,
        schema_overrides=schema,
    )


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(
        (sqlalchemy.exc.TimeoutError, sqlalchemy.exc.OperationalError)
    ),
)
def create_sessions(test_run) -> dict[str, Session]:
    """
    Returns:
        dict: A dictionary containing initialized SessionManager instances for each database session.
    """
    ukrdc_instance = "ukrdc_staging"
    radar_instance = "radar_staging"

    if not test_run:
        ukrdc_instance = "ukrdc_live"
        radar_instance = "radar_live"

    return {
        "ukrdc": PostgresConnection(
            app=ukrdc_instance, tunnel=True, via_app=True
        ).session(),
        "radar": PostgresConnection(
            app=radar_instance, tunnel=True, via_app=True
        ).session(),
        # Currently no staging server for RR
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

    return rr_df


def get_modality_codes(session: Session) -> pl.DataFrame:
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
    return get_data_as_df(session, query).drop_nulls()


def get_satellite_map(session: Session) -> pl.DataFrame:
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


def df_insert_to_sql(
    dataframe: pl.DataFrame,
    session: Session,
    table: str,
):
    conn = session.connection()

    try:
        session.rollback()
        session.begin()

        # Count existing rows before insert
        total_before = conn.execute(text("SELECT COUNT(*) FROM transplants;")).scalar()

        # Insert dataframe using Polars
        dataframe.write_database(
            table_name=table,
            if_table_exists="append",
            connection=conn,
        )
        # Count rows after insert
        total_after = conn.execute(text("SELECT COUNT(*) FROM transplants;")).scalar()

        inserted = total_after - total_before

        # Validate
        if inserted != dataframe.height:
            raise SQLAlchemyError(
                f"Inserted {inserted} rows but dataframe has {dataframe.height}"
            )

        session.commit()

        return inserted

    except Exception as e:
        # Rollback on any error
        session.rollback()
        raise e


def df_batch_update_to_sql(
    dataframe: pl.DataFrame,
    session: Session,
    table_model,
    batch_size: int = 1000,
):
    # Convert to dict rows
    df_rows = dataframe.to_dicts()

    # Ensure only valid table columns are included
    mapper = inspect(table_model)
    valid_columns = {col.key for col in mapper.columns}

    # Split into batches
    batches = [df_rows[i : i + batch_size] for i in range(0, len(df_rows), batch_size)]

    total_updated = 0

    try:
        for batch in batches:
            # Filter each row so SQLAlchemy only sees valid columns
            clean_batch = [
                {k: v for k, v in row.items() if k in valid_columns} for row in batch
            ]

            # Perform bulk update
            session.bulk_update_mappings(table_model, clean_batch)
            total_updated += len(clean_batch)

        session.commit()
        return total_updated

    except Exception as e:
        session.rollback()
        raise e
