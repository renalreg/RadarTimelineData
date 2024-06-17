import polars as pl
import radar_models.radar2 as radar2
from sqlalchemy.orm import Session

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.connections import (
    sessions_to_treatment_dfs,
    get_source_group_id_mapping,
    df_batch_insert_to_sql,
)
from radar_timeline_data.utils.treatment_utils import (
    group_and_reduce_ukrdc_dataframe,
    combine_treatment_dataframes,
    fill_null_time,
    split_combined_dataframe,
    group_and_reduce_combined_treatment_dataframe,
    format_treatment,
)


def treatment_run(
    audit_writer: AuditWriter | StubObject,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    sessions: dict[str, Session],
    radar_patient_id_map: pl.DataFrame,
    commit: bool = False,
) -> None:
    """
    Runs the treatment data processing pipeline.

    Args:
        audit_writer: An AuditWriter or StubObject instance for logging.
        codes: DataFrame containing treatment codes.
        satellite: DataFrame containing satellite data.
        sessions: Dictionary of Session objects.
        radar_patient_id_map: DataFrame mapping radar patient IDs.
        commit: Flag indicating whether to commit data to the database.

    Returns:
        None
    """

    # =====================< GET TREATMENTS >==================
    df_collection = sessions_to_treatment_dfs(
        sessions,
        radar_patient_id_map.filter(pl.col("ukrdcid").is_not_null()).get_column(
            "ukrdcid"
        ),
    )

    audit_writer.add_text("Importing Treatment data from:")
    audit_writer.set_ws(worksheet_name="treatment_import")
    audit_writer.add_table(
        text="  UKRDC", table=df_collection["ukrdc"], table_name="treatment_ukrdc"
    )
    audit_writer.add_table(
        text="  RADAR", table=df_collection["radar"], table_name="treatment_radar"
    )
    cols = df_collection["ukrdc"].head()
    source_group_id_mapping = get_source_group_id_mapping(sessions["radar"])

    # =====================< Formatting >==================

    df_collection = format_treatment(
        codes,
        df_collection,
        satellite,
        source_group_id_mapping,
        radar_patient_id_map,
        audit_writer,
    )

    audit_writer.add_change(
        "Converting ukrdc into common formats, includes patient numbers and modality codes ",
        [cols, df_collection["ukrdc"].head()],
    )
    audit_writer.add_table(
        text="UKRDC treatments in RADAR format",
        table=df_collection["ukrdc"],
        table_name="format_ukrdc",
    )
    # clean up
    del codes, satellite, cols

    # =====================< REDUCE >==================

    audit_writer.add_text(
        "After formatting treatments in radar format similar treatments need to be Aggregated"
    )
    audit_writer.set_ws("group_reduce_Treatment")
    df_collection["ukrdc"] = group_and_reduce_ukrdc_dataframe(
        df_collection, audit_writer
    )

    # =====================< MERGE  >==================

    # combine all dataframes into one
    combined_dataframe = combine_treatment_dataframes(df_collection)

    audit_writer.set_ws("raw_all_Treatment")
    audit_writer.add_table(
        text="Combine data from UKRDC and RADAR",
        table=combined_dataframe,
        table_name="raw_combined_Treatment",
    )

    # clean up
    for frame in df_collection:
        df_collection[frame].clear()
    del df_collection

    audit_writer.set_ws("group_reduce_all_Treatment")
    audit_writer.add_text(
        "The data is now consolidated into one table and requires grouping and aggregation."
    )

    # =====================< REDUCE >==================

    # group the combined dataframe and reduce into the first occurrence for each patient-group combination
    reduced_dataframe = group_and_reduce_combined_treatment_dataframe(
        combined_dataframe
    )
    audit_writer.add_table(
        "All treatments have been grouped and reduced",
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

    audit_writer.add_info(
        "treatments out",
        (
            "total to update/create:",
            str(len(new_treatments) + len(existing_treatments)),
        ),
    )
    audit_writer.add_info(
        "treatments out",
        ("total transplants to update", str(len(existing_treatments))),
    )
    audit_writer.add_info(
        "treatments out",
        ("total transplants to create", str(len(new_treatments))),
    )

    # =====================< WRITE TO DATABASE >==================
    if commit:
        audit_writer.add_text("Starting data commit.")
        total_rows, failed_rows = df_batch_insert_to_sql(
            new_treatments, sessions["radar"], radar2.Dialysi.__table__, 1000, "id"
        )
        audit_writer.add_text(f"{total_rows} rows of treatment data added or modified")

        if len(failed_rows) > 0:
            temp = pl.from_dicts(failed_rows)
            audit_writer.set_ws("errors")
            audit_writer.add_table(
                f"{len(failed_rows)} rows of treatment data failed",
                temp,
                "failed_treatment_rows",
            )
            audit_writer.add_important(
                f"{len(failed_rows)} rows of treatment data insert failed", True
            )
