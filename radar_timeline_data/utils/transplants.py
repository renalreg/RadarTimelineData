import polars as pl
import radar_models.radar2 as radar
import ukrr_models.nhsbt_models as nhsbt
from sqlalchemy.orm import Session
from sqlalchemy import select, cast, String, Date

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.connections import (
    df_batch_insert_to_sql,
    get_data_as_df,
)

from radar_timeline_data.utils.utils import chunk_list


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

    df_collection = make_transplant_dfs(
        sessions,
        radar_patient_id_map.drop_nulls(["rr_no"])
        .unique(subset=["rr_no"])
        .get_column("rr_no"),
    )

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
            text=f"Imported {key} transplants  \u2192 ",
            table=value,
            table_name=f"raw_transplant_{key}",
        )
    audit_writer.add_text(
        "Converting RR transplants into common formats, includes patient numbers and modality codes "
    )

    df_collection = format_transplant(df_collection, radar_patient_id_map, sessions)

    audit_writer.set_ws("transplant_format")
    audit_writer.add_table(
        "RR transplants with radar format  \u2192 ",
        df_collection["rr"],
        "format_rr_table",
    )

    audit_writer.add_text("Grouping and Reducing RR transplants")
    audit_writer.set_ws("reduced")

    df_collection = group_and_reduce_transplant_rr(audit_writer, df_collection)
    audit_writer.add_table(
        "reduced RR transplants", df_collection["rr"], "reduced_rr_transplants"
    )

    audit_writer.add_text("Transplants in RR and RADAR are merged")
    audit_writer.set_ws("transplant_merge")
    all_transplants = pl.concat(
        [df_collection["radar"], df_collection["rr"]], how="diagonal_relaxed"
    )
    audit_writer.add_table(
        "transplants after merge", all_transplants, "merged_transplants"
    )

    audit_writer.add_text(
        "Grouping and Reducing all Transplants by grouping overlapping transplants within 5 days, "
        "prioritising data sources and aggregating essential patient and group information"
    )

    cols = all_transplants.columns

    all_transplants = (all_transplants.sort("patient_id", "date")).with_columns(
        pl.col(col_name).shift().over("patient_id").alias(f"{col_name}_shifted")
        for col_name in cols
    )

    # date mask to define overlapping transplants
    mask = abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    # group using the mask and perform a 'run length encoding'
    all_transplants = all_transplants.with_columns(
        pl.when(mask).then(0).otherwise(1).over("patient_id").alias("group_id")
    )
    all_transplants = all_transplants.with_columns(
        pl.col("group_id").cumsum().rle_id().over("patient_id").alias("group_id")
    )

    # convert source types into priority numbers
    all_transplants = all_transplants.with_columns(
        pl.col("source_type")
        .replace(
            old=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3", "4"],
            default=None,
        )
        .cast(pl.Int32)
    )
    # sort data in regard to source priority
    all_transplants = all_transplants.sort(
        "patient_id", "group_id", "source_type", descending=True
    )
    # group data and aggregate first non-null id and first of other columns per patient and group
    all_transplants = (
        all_transplants.groupby(["patient_id", "group_id"])
        .agg(
            pl.col("id").drop_nulls().first(),
            **{
                col: pl.col(col).first()
                for col in cols
                if col not in ["patient_id", "group_id", "id"]
            },
        )
        .drop(columns=["group_id"])
    )

    # convert source_type back to correct format
    all_transplants = all_transplants.with_columns(
        pl.col("source_type")
        .cast(pl.String)
        .replace(
            new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            old=["0", "1", "2", "3", "4"],
            default=None,
        )
    )

    # =====================< CHECK for Changes  >==================

    new_transplant_rows = all_transplants.filter(pl.col("id").is_null())

    updated_transplant_rows = all_transplants.filter(pl.col("id").is_not_null())

    audit_writer.add_table(
        "reduced transplants", all_transplants, "reduced_transplant_data"
    )
    audit_writer.set_ws("transplant_output")
    audit_writer.add_table(
        "new transplants",
        all_transplants.filter(pl.col("id").is_null()),
        "new_transplant_data",
    )
    audit_writer.add_table(
        "updated transplants",
        all_transplants.filter(pl.col("id").is_not_null()),
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
        total_rows, failed_rows = df_batch_insert_to_sql(
            all_transplants,
            sessions["radar"],
            radar.Transplant.__table__,
            1000,
            "id",
        )
        audit_writer.add_text(f"{total_rows} rows of transplant data added or modified")

        if len(failed_rows) > 0:
            temp = pl.from_dicts(failed_rows)
            audit_writer.set_ws("errors")
            audit_writer.add_table(
                f"{len(failed_rows)} rows of transplant data failed",
                temp,
                "failed_transplant_rows",
            )
            audit_writer.add_important(
                f"{len(failed_rows)} rows of treatment data insert failed", True
            )


def make_transplant_dfs(
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

    # Extract data for "radar" session convert id to string for polars to work

    radar_query = select(
        cast(radar.Transplant.id, String),
        radar.Transplant.patient_id,
        radar.Transplant.modality,
        radar.Transplant.date,
        radar.Transplant.date_of_failure,
        radar.Transplant.source_group_id,
        # radar.Transplant.hla_mismatch # Uncomment when added
    )

    df_collection = {"radar": get_data_as_df(sessions["radar"], radar_query)}

    str_filter = rr_filter.to_list()

    df_collection["rr"] = pl.DataFrame()

    for chunk in chunk_list(str_filter, 1000):
        rr_query = (
            select(
                nhsbt.UKTTransplant.rr_no.label("patient_id"),
                nhsbt.UKTTransplant.transplant_type.label("modality"),
                cast(nhsbt.UKTTransplant.transplant_date, Date).label("date"),
                cast(nhsbt.UKTTransplant.ukt_fail_date, Date).label("date_of_failure"),
                # nhsbt.UKTTransplant.hla_mismatch, # Uncomment when added to radar
                nhsbt.UKTTransplant.transplant_relationship,
                nhsbt.UKTTransplant.transplant_sex,
                nhsbt.UKTSites.rr_code.label("source_group_id"),
            )
            .join(
                nhsbt.UKTSites,
                nhsbt.UKTTransplant.transplant_unit == nhsbt.UKTSites.site_name,
            )
            .filter(nhsbt.UKTTransplant.rr_no.in_(chunk))
        )
        df_chunk = get_data_as_df(sessions["rr"], rr_query)
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
        pl.col(col_name).shift().over("patient_id").alias(f"{col_name}_shifted")
        for col_name in cols
    )
    mask = abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    df_collection["rr"] = df_collection["rr"].with_columns(
        pl.when(mask).then(0).otherwise(1).over("patient_id").alias("group_id")
    )
    df_collection["rr"] = df_collection["rr"].with_columns(
        pl.col("group_id").cumsum().rle_id().over("patient_id").alias("group_id")
    )
    audit_writer.add_table(
        "Transplants from RR grouped based on patient_id  \u2192 ",
        df_collection["rr"],
        "grouped_rr",
    )

    df_collection["rr"] = (
        df_collection["rr"]
        .groupby(["patient_id", "group_id"])
        .agg(
            **{
                col: pl.col(col).first()
                for col in cols
                if col not in ["patient_id", "group_id"]
            }
        )
        .drop("group_id")
        .with_columns(pl.lit(None, pl.String).alias("id"))
    )
    audit_writer.add_table(
        "Transplants from RR aggregated by first values in each group, no priority given  \u2192 ",
        df_collection["rr"],
        "reduced_rr",
    )
    return df_collection


def format_transplant(
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
        patient_id=pl.col("patient_id").replace(
            rr_map.get_column("rr_no"),
            rr_map.get_column("radar_id"),
            default="None",
        )
    )
    # convert transplant unit to radar int code
    df_collection = convert_transplant_unit(df_collection, sessions)
    df_collection["rr"] = get_rr_transplant_modality(df_collection["rr"])
    df_collection["rr"] = (
        df_collection["rr"]
        .drop(
            [
                "transplant_relationship",
                "transplant_sex",
            ]
        )
        .with_columns(
            pl.lit(200).alias("source_group_id"), pl.lit("RR").alias("source_type")
        )
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

    ttype = pl.col("modality")
    alive = ttype.is_in(["Live"])
    dead = ttype.is_in(["DCD", "DBD"])
    trel = pl.col("transplant_relationship")
    tsex = pl.col("transplant_sex")
    father = "1"
    mother = "2"
    # TODO missing 25 to 28
    rr_df = rr_df.with_columns(
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
        .alias("modality")
    ).cast({"modality": pl.Int64})

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
        source_group_id=pl.col("source_group_id").replace(
            kmap.get_column("code"),
            kmap.get_column("id"),
            default=None,
        )
    )

    return df_collection
