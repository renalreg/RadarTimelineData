import pandas
import polars as pl
import radar_models.radar2 as radar2
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import elements

from sqlalchemy.orm import Session

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.connections import (
    sessions_to_treatment_dfs,
    get_source_group_id_mapping,
    export_to_sql,
)
from radar_timeline_data.utils.treatment_utils import (
    group_and_reduce_ukrdc_dataframe,
    combine_treatment_dataframes,
    fill_null_time,
    split_combined_dataframe,
    group_and_reduce_combined_treatment_dataframe,
    format_treatment,
)


def if_key_exists(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],  # Specify the primary key column(s)
        set_=dict(
            (col, stmt.excluded[col]) for col in data[0].keys()
        ),  # Update all columns
    )
    result = conn.execute(stmt)
    return result.rowcount


def treatment_run(
    audit_writer: AuditWriter | StubObject,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    sessions: dict[str, Session],
    radar_patient_id_map: pl.DataFrame,
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
        sessions,
        radar_patient_id_map.filter(pl.col("ukrdcid").is_not_null()).get_column(
            "ukrdcid"
        ),
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
        codes, df_collection, satellite, source_group_id_mapping, radar_patient_id_map
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
    del codes, satellite, cols

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
    reduced_dataframe = group_and_reduce_combined_treatment_dataframe(
        combined_dataframe
    )
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

    new_treatments = new_treatments.slice(0, 1)
    new_treatments = new_treatments.drop(
        ["source_type", "id", "created_user_id", "modified_user_id", "recent_date"]
    ).with_columns(
        pl.lit("b91d66f2-cd53-42ec-82f8-8d52de5b5bbc").alias("id"),
        pl.lit("DEL").alias("source_type"),
        pl.lit(999).alias("created_user_id"),
        pl.lit(100).alias("modified_user_id"),
    )

    print(new_treatments)

    new_treatments: pandas.DataFrame = new_treatments.to_pandas()

    new_treatments.to_sql(
        name=radar2.Dialysi.__tablename__,
        con=sessions["radar"].bind,
        if_exists="append",
        index=False,
        chunksize=1,
        method=if_key_exists,
    )

    if commit:
        export_to_sql(
            session=sessions["radar"],
            data=new_treatments,
            tablename=radar2.Dialysi,
            contains_pk=True,
        )
    else:
        return