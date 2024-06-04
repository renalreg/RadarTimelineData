"""
TimeLineData importer script.

This script handles the import and processing of timeline data, including treatment and transplant data.

"""

from datetime import datetime

import polars as pl
from loguru import logger
from sqlalchemy.orm import Session

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.args import get_args
from radar_timeline_data.utils.connections import (
    map_ukrdcid_to_radar_number,
    sessions_to_treatment_dfs,
    create_sessions,
    get_modality_codes,
    get_sattelite_map,
    get_source_group_id_mapping,
    sessions_to_transplant_dfs,
    get_rr_to_radarnumber_map,
    export_to_sql,
)
from radar_timeline_data.utils.polarUtil import (
    group_and_reduce_ukrdc_dataframe,
    combine_treatment_dataframes,
    fill_null_time,
    split_combined_dataframe,
    group_and_reduce_combined_dataframe,
    format_treatment,
    get_rr_transplant_modality,
    convert_transplant_unit,
)


def main(
    audit_writer: AuditWriter | StubObject = StubObject(),
    commit: bool = False,
    test_run: bool = False,
    max_data_lifetime: int | None = None,
) -> None:
    """
    main function for flow of script
    Args:
        audit_writer: Object used for writing readable audit files
        commit: boolean to indicate whether or not to commit
        test_run: boolean to indicate whether or not to run on test databases
        max_data_lifetime: maximum age of data

    Returns:

    """

    # =======================< START >====================

    audit_writer.add_text("starting script", style="Heading 4")
    sessions = create_sessions()

    codes = get_modality_codes(sessions)
    satellite = get_sattelite_map(sessions["ukrdc"])
    ukrdc_radar_mapping = map_ukrdcid_to_radar_number(sessions)

    # write tables to audit
    audit_writer.set_ws(worksheet_name="mappings")
    audit_writer.add_table(
        text="Modality Codes:", table=codes, table_name="Modality_Codes"
    )
    audit_writer.add_table(
        text="Satellite Units:", table=satellite, table_name="Satellite_Units"
    )

    audit_writer.add_table(
        text="Patient number mapping:",
        table=ukrdc_radar_mapping,
        table_name="Patient_number",
    )

    # =======================< TRANSPLANT AND TREATMENT RUNS >====================
    audit_writer.add_text("Starting Treatment Run", "Heading 3")
    treatment_run(audit_writer, codes, satellite, sessions, ukrdc_radar_mapping, commit)
    del ukrdc_radar_mapping, codes

    audit_writer.add_text("Starting Transplant Run", "Heading 3")

    rr_radar_mapping = get_rr_to_radarnumber_map(sessions)

    transplant_run(audit_writer, sessions, rr_radar_mapping)

    # send to database
    # close the sessions connection
    for session in sessions.values():
        session.close()


def transplant_run(
    audit_writer: AuditWriter | StubObject,
    sessions: dict[str, Session],
    rr_radar_mapping: pl.DataFrame,
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
        sessions, rr_radar_mapping.get_column("number")
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

    df_collection = format_transplant(df_collection, rr_radar_mapping, sessions)

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

    # ['id', 'patient_id', 'source_group_id', 'source_type', 'transplant_group_id', 'date', 'modality', 'date_of_recurrence', 'date_of_failure', 'created_user_id', 'created_date', 'modified_user_id', 'modified_date', 'recurrence', 'date_of_cmv_infection', 'donor_hla', 'recipient_hla', 'graft_loss_cause']
    # [Object, Int64, Int64, String, Int64, Date, Int64, Date, Date, Int64, Datetime(time_unit='us', time_zone=None), Int64, Datetime(time_unit='us', time_zone=None), Boolean, Date, String, String, String]
    # ['patient_id', 'rr_no', 'date', 'date_of_failure', 'hla_mismatch', 'transplant_group_id', 'modality', 'source_group_id', 'source_type', 'id']
    # [String, Int64, Datetime(time_unit='us', time_zone=None), Datetime(time_unit='us', time_zone=None), String, Int64, Int32, Int32, String, String]
    a = df_collection["rr"]
    b = df_collection["radar"]
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
) -> dict[str : pl.DataFrame]:
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
    df_collection: dict[str, pl.DataFrame], rr_radar_mapping, sessions
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

    df_collection["rr"] = (
        df_collection["rr"]
        .with_columns(
            patient_id=pl.col("rr_no").replace(
                rr_radar_mapping.get_column("number"),
                rr_radar_mapping.get_column("patient_id"),
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


def treatment_run(
    audit_writer: AuditWriter | StubObject,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    sessions: dict[str, Session],
    ukrdc_radar_mapping: pl.DataFrame,
    commit: bool = False,
) -> None:
    """
    function that controls the flow of treatment rows/data
    Args:
        audit_writer: AuditWriter Object or Stub object for writing dataflow in readable formats
        codes: map of modality codes and their corresponding equivalent
        satellite: map of satellites and main units
        sessions: dictionary of sessions must contain "ukrdc" and "radar"
        ukrdc_radar_mapping: map of ukrdc localpatientid to radar patient_id
        commit: flag to allow for data to be committed
    """

    # =====================< GET TREATMENTS >==================
    df_collection = sessions_to_treatment_dfs(
        sessions, ukrdc_radar_mapping.get_column("number")
    )

    audit_writer.add_text("importing Treatment data from:")
    audit_writer.set_ws(worksheet_name="import")
    audit_writer.add_table(
        text="  UKRDC", table=df_collection["ukrdc"], table_name="treatment_ukrdc"
    )
    audit_writer.add_table(
        text="  RADAR", table=df_collection["radar"], table_name="treatment_radar"
    )
    cols = df_collection["ukrdc"].columns
    a = df_collection["ukrdc"]
    source_group_id_mapping = get_source_group_id_mapping(sessions["radar"])

    # =====================< Formatting >==================

    df_collection = format_treatment(
        codes, df_collection, satellite, source_group_id_mapping, ukrdc_radar_mapping
    )

    audit_writer.add_change(
        description="converting ukrdc into common formats, includes patient numbers and modality codes ",
        old=cols,
        new=df_collection["ukrdc"].columns,
    )
    audit_writer.add_table(
        text="ukrdc format conversion",
        table=df_collection["ukrdc"],
        table_name="format_ukrdc",
    )
    # clean up
    del codes, ukrdc_radar_mapping, satellite, cols

    # =====================< REDUCE >==================

    audit_writer.set_ws("group_reduce_Treatment")
    df_collection["ukrdc"] = group_and_reduce_ukrdc_dataframe(
        df_collection, audit_writer
    )

    # =====================< MERGE  >==================

    # combine all dataframes into one
    combined_dataframe = combine_treatment_dataframes(df_collection)

    audit_writer.set_ws("raw_all_Treatment")
    audit_writer.add_table(
        text="combine dataframes",
        table=combined_dataframe,
        table_name="raw_combined_Treatment",
    )

    # clean up
    for frame in df_collection:
        df_collection[frame].clear()
    del df_collection

    audit_writer.set_ws("group_reduce_all_Treatment")

    # =====================< REDUCE >==================

    # group the combined dataframe and reduce into the first occurrence for each patient-group combination
    reduced_dataframe = group_and_reduce_combined_dataframe(combined_dataframe)
    audit_writer.add_table(
        "reducing_combined_Treatment",
        reduced_dataframe,
        table_name="reduced_combined_Treatment",
    )

    # =====================< SPLIT >==================
    # split treatments

    existing_treatments, new_treatments = split_combined_dataframe(
        combined_dataframe, reduced_dataframe
    )
    # clean up
    del combined_dataframe, reduced_dataframe

    audit_writer.set_ws("Treatment_output")
    # TODO may not be needed as db defaults time
    new_treatments, existing_treatments = fill_null_time(
        new_treatments, existing_treatments
    )
    audit_writer.add_table(
        text="data that is new", table=new_treatments, table_name="new_Treatment"
    )
    audit_writer.add_table(
        text="data to update", table=existing_treatments, table_name="update_Treatment"
    )

    # =====================< WRITE TO DATABASE >==================

    return

    new_treatments = new_treatments.slice(0, 1)
    new_treatments = new_treatments.drop(
        ["source_type", "id", "created_user_id", "modified_user_id", "recent_date"]
    ).with_columns(
        pl.lit("b91d66f2-cd53-42ec-82f8-8d52de5b5bbc").alias("id"),
        pl.lit("REP").alias("source_type"),
        pl.lit(100).alias("created_user_id"),
        pl.lit(100).alias("modified_user_id"),
    )

    if commit:
        export_to_sql(
            session=sessions["radar"],
            data=new_treatments,
            tablename="dialysis",
            contains_pk=True,
        )
    else:
        return


if __name__ == "__main__":
    logger.info("script start")
    args = get_args()

    # Setting up parameters
    params = {}
    audit = (
        AuditWriter(
            f"{args.audit}", "delta", include_excel=True, include_breakdown=True
        )
        if args.audit
        else StubObject()
    )
    params["audit_writer"] = audit

    if args.commit:
        params["commit"] = args.commit
    if args.test_run:
        params["test_run"] = args.test_run

    logger.info(f"Auditing directory: {args.audit}") if args.audit else None

    # Recording start time
    start_time = datetime.now()
    audit.add_info("start time", start_time.strftime("%Y-%m-%d %H:%M"))

    # Calling main function
    main(**params)

    # Recording end time
    end_time = datetime.now()
    audit.add_info("end time", end_time.strftime("%Y-%m-%d %H:%M"))

    # Calculating and recording total time
    total_seconds = (end_time - start_time).total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    audit.add_info("total time", f"{hours} hours {minutes} mins {int(seconds)} seconds")
    audit.commit_audit()

    # Logging script completion
    logger.success(
        f"script finished in {hours} hours {minutes} mins {int(seconds)} seconds"
    )
