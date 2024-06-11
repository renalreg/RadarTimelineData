import polars as pl
from sqlalchemy.orm import Session

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.connections import sessions_to_transplant_dfs
from radar_timeline_data.utils.transplant_utils import (
    get_rr_transplant_modality,
    convert_transplant_unit,
)


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
    # =====================<IMPORT TRANSPLANT DATA>==================

    # get transplant data from sessions where radar number

    df_collection = sessions_to_transplant_dfs(
        sessions,
        radar_patient_id_map.drop_nulls(["rr_no"])
        .unique(subset=["rr_no"])
        .get_column("rr_no"),
    )
    audit_writer.add_info(
        "transplant", ("rr data loaded", str(len(df_collection["rr"])))
    )
    audit_writer.add_info(
        "transplant", ("radar data loaded", str(len(df_collection["radar"])))
    )
    if df_collection["rr"].is_empty():
        audit_writer.add_text("ukrr no transplants to import")
        return None

    audit_writer.set_ws("import_transplant_run")
    for key, value in df_collection.items():
        audit_writer.add_table(
            text=f"import table {key}",
            table=value,
            table_name=f"raw_transplant_{key}",
        )

    # =====================<FORMAT DATA>==================
    audit_writer.add_text("formatting transplant data")

    df_collection = format_transplant(df_collection, radar_patient_id_map, sessions)

    audit_writer.set_ws("transplant_format")
    audit_writer.add_table("format changes", df_collection["rr"], "format_rr_table")

    # =====================<GROUP AND REDUCE>==================
    audit_writer.add_text("Group and Reduce")
    audit_writer.set_ws("reduced")

    df_collection = group_and_reduce_transplant_rr(audit_writer, df_collection)

    # =====================< COMBINE RADAR & RR >==================

    audit_writer.add_text("merging transplants data")
    audit_writer.set_ws("transplant_merge")
    audit_writer.add_table(
        "rr transplants before merge", df_collection["rr"], "unmerged_rr_transplants"
    )
    audit_writer.add_table(
        "radar transplants before merge",
        df_collection["radar"],
        "unmerged_radar_transplants",
    )

    combine_df = pl.concat(
        [df_collection["radar"], df_collection["rr"]], how="diagonal_relaxed"
    )

    audit_writer.add_table("transplants after merge", combine_df, "merged_transplants")

    # =====================< GROUP AND REDUCE >==================
    audit_writer.add_text("grouping and reducing merged transplants")
    # list of current columns
    cols = combine_df.columns
    # shift columns
    combine_df = (combine_df.sort("patient_id", "date")).with_columns(
        pl.col(col_name).shift().over("patient_id").alias(f"{col_name}_shifted")
        for col_name in cols
    )

    # date mask to define overlapping transplants
    mask = abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    # group using the mask and perform a 'run length encoding'
    combine_df = combine_df.with_columns(
        pl.when(mask).then(0).otherwise(1).over("patient_id").alias("group_id")
    )
    combine_df = combine_df.with_columns(
        pl.col("group_id").cumsum().rle_id().over("patient_id").alias("group_id")
    )

    # convert source types into priority numbers
    combine_df = combine_df.with_columns(
        pl.col("source_type")
        .replace(
            old=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3", "4"],
            default=None,
        )
        .cast(pl.Int32)
    )
    # sort data in regard to source priority
    combine_df = combine_df.sort(
        "patient_id", "group_id", "source_type", descending=True
    )
    # group data and aggregate first non-null id and first of other columns per patient and group
    combine_df = (
        combine_df.groupby(["patient_id", "group_id"])
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
    combine_df = combine_df.with_columns(
        pl.col("source_type")
        .cast(pl.String)
        .replace(
            new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            old=["0", "1", "2", "3", "4"],
            default=None,
        )
    )

    # =====================< CHECK for Changes  >==================

    new_transplant_rows = combine_df.filter(pl.col("id").is_null())

    updated_transplant_rows = combine_df.filter(pl.col("id").is_not_null())

    audit_writer.add_table("reduced data", combine_df, "reduced_transplant_data")
    audit_writer.set_ws("transplant_output")
    audit_writer.add_table(
        "new transplants",
        combine_df.filter(pl.col("id").is_null()),
        "new_transplant_data",
    )
    audit_writer.add_table(
        "updated transplants",
        combine_df.filter(pl.col("id").is_not_null()),
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

    if combine_df.filter(
        ~pl.col("source_type").is_in(["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"])
    ).get_column("source_type").shape != (0,):
        raise ValueError("source_type")
    if not combine_df.filter(pl.col("patient_id").is_null()).is_empty():
        raise ValueError("patient_id")

    # =====================< WRITE TO DATABASE >==================

    # TODO check that rr ids are in radar by querying


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
        "transplants from rr grouped", df_collection["rr"], "grouped_rr"
    )
    audit_writer.add_text("reducing rr transplants data ...")
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
        "reduced rr transplants :", df_collection["rr"], "reduced_rr"
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

    df_collection["rr"] = (
        df_collection["rr"]
        .with_columns(
            patient_id=pl.col("rr_no").replace(
                rr_map.get_column("rr_no"),
                rr_map.get_column("radar_id"),
                default="None",
            )
        )
        .drop("RR_NO")
    )
    # convert transplant unit to radar int code
    df_collection = convert_transplant_unit(df_collection, sessions)
    df_collection["rr"] = get_rr_transplant_modality(df_collection["rr"])
    df_collection["rr"] = (
        df_collection["rr"]
        .rename(
            {
                "TRANSPLANT_UNIT": "transplant_group_id",
                "ukt_fail_date": "date_of_failure",
                "transplant_date": "date",
                "hla_mismatch": "hla_mismatch",
            }
        )
        .drop(
            [
                "transplant_type",
                "transplant_organ",
                "transplant_relationship",
                "transplant_sex",
            ]
        )
        .with_columns(
            pl.lit(200).alias("source_group_id"), pl.lit("RR").alias("source_type")
        )
    )
    return df_collection
