from functools import reduce
from operator import or_

import polars as pl
import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc
import ukrr_models.nhsbt_models as nhsbt
from polars import DataFrame
from sqlalchemy import select, cast, String, Date
from sqlalchemy.orm import Session

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.config import rr_to_radar_columns
from radar_timeline_data.utils.connections import (
    get_data_as_df,
    df_batch_update_to_sql,
    df_insert_to_sql,
)
from radar_timeline_data.utils.utils import chunk_list
from ukrdc_sqla.ukrdc import column_names as column
from ukrr_models.nhsbt_models import UKTTransplant, UKTSites


def filter_updated(radar_df: DataFrame, updated_transplant_rows):
    # 1. Join updated rows with original on 'id'
    comparison = updated_transplant_rows.join(
        radar_df, on="id", how="left", suffix="_orig"
    )
    cols = radar_df.columns

    # 2. Keep only rows where any column is different
    cols_to_check = [col for col in cols if col != "id"]

    # Build expression: keep row if any column differs from original
    filter_expr = None
    for col in cols_to_check:
        expr = pl.col(col) != pl.col(f"{col}_orig")
        filter_expr = expr if filter_expr is None else filter_expr | expr

    # 3. Filter to get only truly changed rows
    truly_updated_rows = comparison.filter(filter_expr)

    # 4. Drop the original comparison columns to return to clean format
    truly_updated_rows = truly_updated_rows.select(updated_transplant_rows.columns)

    # TODO: ANDY this currently removes unchanged rows this means if sourcetype diffrences can overwrite
    # ie same row different source type should we include this or should it be removed?

    return truly_updated_rows


def transplant_run(
    audit_writer: AuditWriter | StubObject,
    sessions: dict[str, Session],
    radar_patient_id_map: pl.DataFrame,
    commit: bool = False,
):
    """
    Run the transplant data processing pipeline.

    Args:
        audit_writer: AuditWriter or StubObject instance for writing audit logs.
        sessions: Dictionary of session managers.
        rr_radar_mapping: DataFrame containing RR radar mapping data.

    Returns:
        None

    Raises:
        ValueError: If source_type or patient_id fails sanity checks.
    """

    df_collection = make_transplant_dfs(sessions, radar_patient_id_map)

    audit_writer.add_text("Transplant Process", "Heading 3")
    audit_writer.add_info(
        "transplant", ("rr data loaded", str(len(df_collection["rr"])))
    )
    audit_writer.add_info(
        "transplant", ("radar data loaded", str(len(df_collection["radar"])))
    )
    audit_writer.set_ws("import_transplant_run")

    for key, value in df_collection.items():
        audit_writer.add_table(
            text=f"Imported {key} transplants",
            table=value,
            table_name=f"raw_transplant_{key}",
        )
    audit_writer.add_text(
        "Converting RR transplants into common formats, includes patient numbers and modality codes "
    )

    df_collection = format_rr_transplants(df_collection, radar_patient_id_map, sessions)

    audit_writer.set_ws("transplant_format")
    audit_writer.add_table(
        "RR transplants with radar format ",
        df_collection["rr"],
        "format_rr_table",
    )

    audit_writer.add_text("Grouping and Reducing RR transplants")
    audit_writer.set_ws("reduced")

    # TODO: ANDY this commented code Groups transplant records that occur within 5 days of each other,Ensures grouping is done per patient and modality should this be kept
    # df_collection = group_and_reduce_transplant_rr(audit_writer, df_collection)
    # audit_writer.add_table(
    #    "each group within patient_id and modality combinations have been reduced to one row per group",
    #   df_collection["rr"],
    #   "reduced_rr_transplants",
    # )

    audit_writer.add_text("Transplants in RR and RADAR are merged")
    audit_writer.set_ws("combined_transplants")

    filtered_rr_to_radar = {
        k: v
        for k, v in rr_to_radar_columns.items()
        if v not in df_collection["rr"].columns
    }

    # adjust any column names before merging
    df_collection["rr"] = df_collection["rr"].rename(filtered_rr_to_radar)
    common_cols = [
        col
        for col in df_collection["radar"].columns
        if col in df_collection["rr"].columns
    ]

    df_collection["rr"] = df_collection["rr"].select(common_cols)

    all_transplants = pl.concat(
        [df_collection["radar"], df_collection["rr"]], how="diagonal_relaxed"
    )

    audit_writer.add_table(
        "transplants from radar and rr have been combined into one table",
        all_transplants,
        "all_transplants",
    )

    audit_writer.add_text(
        "Grouping and Reducing all Transplants by grouping overlapping transplants within 5 days, "
        "prioritising data sources and aggregating essential patient and group information"
    )

    # 1. Convert source_type into numeric priority
    all_transplants = all_transplants.with_columns(
        pl.col("source_type")
        .replace(
            old=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3", "4"],
        )
        .cast(pl.Int32)
    )

    # 2. Sort so highest priority (largest number) comes first
    all_transplants = all_transplants.sort(
        ["patient_id", "modality", "date", "source_type"],
        descending=[False, False, False, True],  # Only priority sorted descending
    )

    # 3. Group by patient, modality, date and select highest-priority row
    all_transplants = all_transplants.group_by(
        column(
            [
                radar.Transplant.patient_id,
                radar.Transplant.modality,
                radar.Transplant.date,
            ]
        ),
        maintain_order=True,
    ).agg(
        pl.col("id").drop_nulls().first(),  # best ID
        **{
            col: pl.col(col).first()
            for col in all_transplants.columns
            if col
            not in column(
                [
                    radar.Transplant.patient_id,
                    radar.Transplant.modality,
                    radar.Transplant.date,
                    radar.Transplant.id,
                ]
            )
        },
    )

    # 4. Convert source_type back to text labels
    all_transplants = all_transplants.with_columns(
        pl.col(column(radar.Transplant.source_type))
        .cast(pl.String)
        .replace(
            old=["0", "1", "2", "3", "4"],
            new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
        )
    )

    # =====================< CHECK for Changes  >==================

    new_transplant_rows = (
        all_transplants.filter(pl.col(column(radar.Transplant.id)).is_null())
        .drop(column(radar.Transplant.id))
        .with_columns(
            pl.lit(1).alias(column(radar.Transplant.created_user_id)),
            pl.lit(1).alias(column(radar.Transplant.modified_user_id)),
        )
    )

    updated_transplant_rows = all_transplants.filter(
        pl.col(column(radar.Transplant.id)).is_not_null()
    )

    updated_transplant_rows = filter_updated(
        df_collection["radar"], updated_transplant_rows
    ).with_columns(
        pl.lit(1).alias(column(radar.Transplant.created_user_id)),
        pl.lit(1).alias(column(radar.Transplant.modified_user_id)),
    )

    # TODO: ANDY the above are setting to 1 currenlty is there a specifc user code that i Should use or create to aid in tracking this
    # Identify rows where any column has updated values

    audit_writer.add_table(
        "reduced transplants", all_transplants, "reduced_transplant_data"
    )
    audit_writer.set_ws("transplant_output")
    audit_writer.add_table(
        "new transplants",
        new_transplant_rows,
        "new_transplant_data",
    )
    audit_writer.add_table(
        "updated transplants",
        updated_transplant_rows,
        "updated_transplant_data",
    )

    audit_writer.add_info(
        "transplants out",
        (
            "total to update/create:",
            str(len(new_transplant_rows) + len(updated_transplant_rows)),
        ),
    )
    audit_writer.add_info(
        "transplants out",
        ("total transplants to update", str(len(updated_transplant_rows))),
    )
    audit_writer.add_info(
        "transplants out",
        ("total transplants to create", str(len(new_transplant_rows))),
    )

    # =====================< SANITY CHECKS  >==================

    if all_transplants.filter(
        ~pl.col("source_type").is_in(["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"])
    ).get_column("source_type").shape != (0,):
        raise ValueError("source_type")
    if not all_transplants.filter(pl.col("patient_id").is_null()).is_empty():
        raise ValueError("patient_id")

    # =====================< WRITE TO DATABASE >==================
    if commit:
        audit_writer.add_text("Writing Transplant data to database")
        new_total_rows = df_insert_to_sql(
            new_transplant_rows,
            sessions["radar"],
            radar.Transplant.__tablename__,
        )
        updated_total_rows = df_batch_update_to_sql(
            updated_transplant_rows,
            sessions["radar"],
            radar.Transplant,
            1000,
        )

        audit_writer.add_text(
            f"{new_total_rows+updated_total_rows} rows of transplant data added or modified"
        )


def make_transplant_dfs(
    sessions: dict[str, Session], radar_patient_map: DataFrame
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding transplants.

    Args:
        sessions (dict): A dictionary containing session information.
        radar_patient_map:A Dataframe of ids to pull
    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.

    """

    # Extract data for "radar" session convert id to string for polars to work

    rr_filter = (
        radar_patient_map.drop_nulls(["rr_no"])
        .unique(subset=["rr_no"])
        .get_column("rr_no")
    )

    radar_query = select(
        cast(radar.Transplant.id, String),
        radar.Transplant.patient_id,
        radar.Transplant.modality,
        radar.Transplant.date,
        radar.Transplant.date_of_failure,
        radar.Transplant.source_group_id,
        radar.Transplant.source_type,
        radar.Transplant.mismatch_hla,
    )

    df_collection = {
        "radar": get_data_as_df(sessions["radar"], radar_query, [radar.Transplant])
    }

    str_filter = rr_filter.to_list()

    df_collection["rr"] = pl.DataFrame()

    for chunk in chunk_list(str_filter, 1000):
        rr_query = (
            select(
                nhsbt.UKTTransplant.rr_no,
                nhsbt.UKTTransplant.transplant_type,
                nhsbt.UKTTransplant.transplant_date,
                nhsbt.UKTTransplant.ukt_fail_date,
                nhsbt.UKTTransplant.hla_mismatch,  # Uncomment when added to radar
                nhsbt.UKTTransplant.transplant_relationship,
                nhsbt.UKTTransplant.transplant_sex,
                nhsbt.UKTSites.rr_code,
            )
            .join(
                nhsbt.UKTSites,
                nhsbt.UKTTransplant.transplant_unit == nhsbt.UKTSites.site_name,
            )
            .filter(nhsbt.UKTTransplant.rr_no.in_(chunk))
        )
        df_chunk = get_data_as_df(
            sessions["rr"], rr_query, [nhsbt.UKTTransplant, nhsbt.UKTSites]
        )
        df_collection["rr"] = pl.concat([df_collection["rr"], df_chunk])

    return df_collection


def group_and_reduce_transplant_rr(
    audit_writer: AuditWriter | StubObject, df_collection: dict[str, pl.DataFrame]
) -> dict[str, pl.DataFrame]:
    """
    Groups and reduces transplant data from the 'rr' session.

    Args:
        audit_writer: AuditWriter or StubObject instance for writing audit logs.
        df_collection: A dictionary containing DataFrames corresponding to each session.

    Returns:
        pl.DataFrame: The grouped and reduced DataFrame for the 'rr' session.
    """

    cols = df_collection["rr"].columns
    df_collection["rr"] = (df_collection["rr"].sort("patient_id", "date")).with_columns(
        pl.col(col_name)
        .shift()
        .over("patient_id", "modality")
        .alias(f"{col_name}_shifted")
        for col_name in cols
    )
    mask = abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    df_collection["rr"] = df_collection["rr"].with_columns(
        pl.when(mask)
        .then(0)
        .otherwise(1)
        .over("patient_id", "modality")
        .alias("group_id")
    )
    df_collection["rr"] = df_collection["rr"].with_columns(
        pl.col("group_id")
        .cumsum()
        .rle_id()
        .over("patient_id", "modality")
        .alias("group_id")
    )
    audit_writer.add_table(
        "Transplants from RR over patient id and modality with overlapping dates have been grouped  \u2192 ",
        df_collection["rr"].sort("patient_id", "modality", "group_id"),
        "rr_data_with_grouped_ids",
    )

    df_collection["rr"] = (
        df_collection["rr"]
        .groupby(["patient_id", "modality", "group_id"])
        .agg(
            **{
                col: pl.col(col).first()
                for col in cols
                if col not in ["patient_id", "group_id", "modality"]
            }
        )
        .drop("group_id")
        .with_columns(pl.lit(None, pl.String).alias("id"))
    )
    return df_collection


def format_rr_transplants(
    df_collection: dict[str, pl.DataFrame], radar_patient_id_map, sessions
):
    """
    Formats transplant data from the 'rr' session.

    Args:
        df_collection: A dictionary containing DataFrames corresponding to each session.
        rr_radar_mapping: DataFrame containing RR radar mapping data.
        sessions: Dictionary of session managers.

    Returns:
        dict: A dictionary containing the formatted DataFrame for the 'rr' session.
    """

    rr_map = radar_patient_id_map.drop_nulls(["rr_no"]).unique(subset=["rr_no"])

    df_collection["rr"] = df_collection["rr"].with_columns(
        patient_id=pl.col(column(UKTTransplant.rr_no))
        .replace(
            rr_map.get_column("rr_no"),
            rr_map.get_column("radar_id"),
            default=None,
        )
        .cast(pl.Int64)
    )
    # TODO add a check here

    # convert transplant unit to radar int code
    df_collection = convert_transplant_unit(df_collection, sessions)
    df_collection["rr"] = get_rr_transplant_modality(df_collection["rr"])
    df_collection["rr"] = (
        df_collection["rr"]
        .drop(
            column(UKTTransplant.transplant_relationship, UKTTransplant.transplant_sex)
        )
        .with_columns(
            pl.lit(124).alias(column(radar.Transplant.source_group_id)),
            pl.lit("RR").alias(column(radar.Transplant.source_type)),
        )  # TODO: ANDY not sure why im setting source group to 200, should it be missing values set to 200?
    )
    return df_collection


def get_rr_transplant_modality(rr_df: pl.DataFrame) -> pl.DataFrame:
    """
    Get the transplant modality based on specific conditions.

    Args:
        rr_df: pl.DataFrame - A Polars DataFrame containing transplant data.

    Returns:
        pl.DataFrame: A Polars DataFrame with an added column 'modality' representing the transplant modality.

    Examples:
        >>> df = pl.DataFrame({
        ...     "modality": ["Live", "DCD", "Live"],
        ...     "TRANSPLANT_RELATIONSHIP": ["0", "2", "9"],
        ...     "TRANSPLANT_SEX": ["1", "2", "1"]
        ... })
        >>> result = get_rr_transplant_modality(df)
    """

    ttype = pl.col(column(UKTTransplant.transplant_type))
    alive = ttype.is_in(["Live"])
    dead = ttype.is_in(["DCD", "DBD"])
    trel = pl.col(column(UKTTransplant.transplant_relationship))
    tsex = pl.col(column(UKTTransplant.transplant_sex))
    father = "1"
    mother = "2"
    # TODO missing 25 to 28
    rr_df = (
        rr_df.with_columns(
            # child
            pl.when(alive & (trel == "0"))
            .then(77)
            # sibling
            .when(alive & (trel.is_in(["3", "4", "5", "6", "7", "8"])))
            .then(21)
            # father
            .when(alive & (trel == "2") & (tsex == father))
            .then(74)
            # mother
            .when(alive & (trel == "2") & (tsex == mother))
            .then(75)
            # other related
            .when(alive & (trel == "9"))
            .then(23)
            # live unrelated
            .when(alive & (trel.is_in(["11", "12", "15", "16", "19", "10"])))
            .then(24)
            # cadaver donor
            .when(dead)
            .then(20)
            # unknown
            .when(trel.is_in(["88", "99"]))
            .then(99)
            .otherwise(None)
            .alias(column(UKTTransplant.transplant_type))
        )
        .cast({column(UKTTransplant.transplant_type): pl.Int64})
        .filter(pl.col(column(UKTTransplant.transplant_type)).is_not_null())
    )

    return rr_df


def convert_transplant_unit(df_collection, sessions: dict[str, Session]):
    """
    Converts transplant unit codes in a DataFrame using a mapping obtained from a database session.

    Args:
        df_collection: dict - A dictionary containing DataFrames, where 'rr' DataFrame has 'TRANSPLANT_UNIT' column.
        sessions: dict - A dictionary of database sessions, with 'radar' key used to query mapping data.

    Returns:
        dict: A dictionary with updated 'rr' DataFrame containing mapped 'TRANSPLANT_UNIT' values.

    Raises:
        KeyError: If the 'TRANSPLANT_UNIT' column is missing in the 'rr' DataFrame.
    """

    query = select(radar.Group.id, radar.Group.code).filter(
        radar.Group.type == "HOSPITAL"
    )
    kmap = get_data_as_df(sessions["radar"], query)

    df_collection["rr"] = df_collection["rr"].with_columns(
        pl.col(column(UKTSites.rr_code))
        .replace(
            kmap.get_column("code"),
            kmap.get_column("id"),
            default=None,
        )
        .alias(column(radar.Transplant.source_group_id))
    )

    return df_collection


def update_mask(cols):
    conditions = [pl.col(col) != pl.col(f"{col}_old") for col in cols]
    combined_condition = reduce(or_, conditions)
    return combined_condition
