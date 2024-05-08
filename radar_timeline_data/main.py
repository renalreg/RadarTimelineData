"""

"""
import argparse
from datetime import datetime

import polars as pl

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.connections import (
    get_ukrdcid_to_radarnumber_map,
    sessions_to_treatment_dfs,
    create_sessions,
    get_modality_codes,
    get_sattelite_map,
    get_source_group_id_mapping,
    SessionManager,
    sessions_to_transplant_dfs,
    get_rr_to_radarnumber_map,
)
from radar_timeline_data.utils.polarUtil import (
    group_and_reduce_ukrdc_dataframe,
    combine_treatment_dataframes,
    fill_null_time,
    split_combined_dataframe,
    group_and_reduce_combined_dataframe,
    treatment_table_format_conversion, get_rr_transplant_modality, convert_transplant_unit,
)


# TODO delete this when done
def audit():
    population = pl.DataFrame(
        {
            "country": ["United Kingdom", "USA", "United States", "france"],
            "date": [
                datetime(2016, 5, 12),
                datetime(2017, 5, 12),
                datetime(2018, 5, 12),
                datetime(2019, 5, 12),
            ],  # note record date: May 12th (sorted!)
            "population": [82.19, 82.66, 83.12, 83.52],
        }
    )
    test = ["a"]
    test = test.extend("b")
    print(test)
    a = StubObject()
    a = AuditWriter(r"""C:\Users\oliver.reeves\Desktop""", "del")
    a.add_info("items changed", "10")
    a.add_info("items removed", "10")
    a.add_text("starting")
    a.add_text("processing 100 items", True)
    a.set_ws(worksheet_name="start")

    a.add_table_snippets(population)

    a.add_table(text="import table", table=population, table_name="starting_table")
    a.set_ws(worksheet_name="end")
    a.add_table(text="testing ", table=population, table_name="temp2")
    a.add_table(text="testing 2", table=population, table_name="temp3")
    a.add_change("column change", ["a", "b"], ["c"])
    a.add_change("table change", population, population)
    a.add_important(" etes", True)
    a.add_important(" etes", False)
    a.commit_audit()


def main(audit_writer: AuditWriter | StubObject = StubObject()):
    """
    main function for flow of script
    Args:
        audit_writer: Object used for writing readable audit files

    Returns:

    """

    # =======================< START >====================

    audit_writer.add_text("starting script")
    sessions = create_sessions()

    # get codes from ukrdc
    codes = get_modality_codes(sessions)
    satellite = get_sattelite_map(sessions["ukrdc"])

    # write tables to audit
    audit_writer.set_ws(worksheet_name="mappings")
    audit_writer.add_table(
        text="Modality Codes:", table=codes, table_name="Modality_Codes"
    )
    audit_writer.add_table_snippets(codes)
    audit_writer.add_table(
        text="Satellite Units:", table=satellite, table_name="Satellite_Units"
    )
    audit_writer.add_table_snippets(satellite)

    # get healthcare facility mapping
    ukrdc_radar_mapping = get_ukrdcid_to_radarnumber_map(sessions)

    audit_writer.add_table(
        text="Patient number mapping:",
        table=ukrdc_radar_mapping,
        table_name="Patient_number",
    )
    audit_writer.add_table_snippets(ukrdc_radar_mapping)

    # =======================< TRANSPLANT AND TREATMENT RUNS >====================

    # treatment_run(audit_writer, codes, satellite, sessions, ukrdc_radar_mapping)

    rr_radar_mapping = get_rr_to_radarnumber_map(sessions)
    transplant_run(
        audit_writer, codes, satellite, sessions, ukrdc_radar_mapping, rr_radar_mapping
    )

    # send to database
    # close the sessions connection
    for session in sessions.values():
        session.session.close()


def transplant_run(
        audit_writer: AuditWriter | StubObject,
        codes: pl.DataFrame,
        satellite: pl.DataFrame,
        sessions: dict[str, SessionManager],
        ukrdc_radar_mapping: pl.DataFrame,
        rr_radar_mapping: pl.DataFrame,
):
    # =====================<IMPORT TRANSPLANT DATA>==================
    # get transplant data from sessions where radar number
    # TODO check if cause of failure is needed in radar
    df_collection = sessions_to_transplant_dfs(
        sessions,
        ukrdc_radar_mapping.get_column("number"),
        rr_radar_mapping.get_column("number"),
    )

    # =====================<FORMAT DATA>==================

    df_collection["rr"] = df_collection["rr"].with_columns(
        patient_id=pl.col("RR_NO").replace(
            rr_radar_mapping.get_column("number"),
            rr_radar_mapping.get_column("patient_id"),
            default="None",
        )
    ).drop("RR_NO")
    # convert transplant unit to radar int code
    df_collection = convert_transplant_unit(df_collection, sessions)
    df_collection["rr"] = get_rr_transplant_modality(df_collection["rr"])

    df_collection["rr"] = df_collection["rr"].rename(
        {
            "TRANSPLANT_UNIT": "transplant_group_id",
            "UKT_FAIL_DATE": "date_of_failure",
            "TRANSPLANT_DATE": "date",
            "HLA_MISMATCH": "hla_mismatch"
        }
    ).drop(["TRANSPLANT_TYPE", "TRANSPLANT_ORGAN", "TRANSPLANT_RELATIONSHIP", "TRANSPLANT_SEX"]).with_columns(
        pl.lit(200).alias("source_group_id"), pl.lit("RR").alias("source_type"))

    with pl.Config(tbl_cols=-1):
        print(df_collection["rr"].filter(pl.col("modality").is_not_null()))
        print(df_collection["rr"].filter(pl.col("modality").is_null()))

    # =====================<GROUP AND REDUCE>==================

    cols = df_collection["rr"].columns
    df_collection["rr"] = ((df_collection["rr"]
                            .sort("patient_id", "date"))
    .with_columns(
        pl.col(col_name).shift().over("patient_id").alias(f"{col_name}_shifted") for col_name in cols))

    mask = (
            abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    )
    df_collection['rr'] = df_collection["rr"].with_columns(
        pl.when(mask).then(0).otherwise(1).over("patient_id").alias("group_id"))
    df_collection['rr'] = df_collection["rr"].with_columns(
        pl.col("group_id").cumsum().rle_id().over("patient_id").alias("group_id"))

    df_collection['rr'] = df_collection["rr"].groupby(["patient_id", "group_id"]).agg(**{
        col: pl.col(col).first()
        for col in cols
        if col not in ["patient_id", "group_id"]
    }).drop("group_id").with_columns(pl.lit(None, pl.String).alias("id"))

    print(df_collection["radar"].sort("patient_id"))
    print(df_collection["rr"].sort("patient_id"))

    # total 19731

    # =====================< COMBINE RADAR & RR >==================
    combine_df = pl.concat([df_collection["radar"], df_collection["rr"]], how="diagonal_relaxed")

    # =====================< GROUP AND REDUCE >==================

    # list of current columns
    cols = combine_df.columns
    # shift columns
    combine_df = ((combine_df
                   .sort("patient_id", "date"))
    .with_columns(
        pl.col(col_name).shift().over("patient_id").alias(f"{col_name}_shifted") for col_name in cols))

    # date mask to define overlapping transplants
    mask = (
            abs(pl.col("date") - pl.col("date_shifted")) <= pl.duration(days=5)
    )
    # group using the mask and perform a 'run length encoding'
    combine_df = combine_df.with_columns(
        pl.when(mask).then(0).otherwise(1).over("patient_id").alias("group_id"))
    combine_df = combine_df.with_columns(
        pl.col("group_id").cumsum().rle_id().over("patient_id").alias("group_id"))

    # convert source types into priority numbers
    combine_df = combine_df.with_columns(
        pl.col("source_type").replace(
            old=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3", "4"],
            default=None,
        ).cast(pl.Int32)
    )
    # sort data in regard to source priority
    combine_df = combine_df.sort("patient_id", "group_id", "source_type", descending=True)
    # group data and aggregate first non-null id and first of other columns per patient and group
    combine_df = combine_df.groupby(["patient_id", "group_id"]).agg(pl.col("id").drop_nulls().first(), **{
        col: pl.col(col).first()
        for col in cols
        if col not in ["patient_id", "group_id", "id"]
    }).drop(columns=["group_id"])

    # convert source_type back to correct format
    combine_df = combine_df.with_columns(
        pl.col("source_type").cast(pl.String).replace(
            new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            old=["0", "1", "2", "3", "4"],
            default=None,
        )
    )

    # =====================< SANITY CHECKS  >==================
    print(combine_df.filter(~pl.col("source_type").is_in(["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"])).get_column(
        "source_type").shape)
    if combine_df.filter(~pl.col("source_type").is_in(["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"])).get_column(
            "source_type").shape != (0,):
        raise ValueError("source_type")
    if not combine_df.filter(pl.col("patient_id").is_null()).is_empty():
        raise ValueError("patient_id")

    # =====================< WRITE TO DATABASE >==================

    with pl.Config(tbl_cols=-1):
        print(combine_df.filter(pl.col("id").is_null()))
        print(combine_df.filter(pl.col("id").is_not_null()))

    # TODO check that rr ids are in radar by querying
    pass


def treatment_run(audit_writer: AuditWriter | StubObject, codes: pl.DataFrame, satellite: pl.DataFrame,
                  sessions: dict[str, SessionManager], ukrdc_radar_mapping: pl.DataFrame) -> None:
    """
    function that controls the flow of treatment rows/data
    Args:
        audit_writer: AuditWriter Object or Stub object for writing dataflow in readable formats
        codes: map of modality codes and their corresponding equivalent
        satellite: map of satellites and main units
        sessions: dictionary of sessions must contain "ukrdc" and "radar"
        ukrdc_radar_mapping: map of ukrdc localpatientid to radar patient_id
    """

    # =====================< GET TREATMENTS >==================
    df_collection = sessions_to_treatment_dfs(
        sessions, ukrdc_radar_mapping.get_column("number")
    )

    audit_writer.add_text("importing Treatment data from:")
    audit_writer.set_ws(worksheet_name="import")
    audit_writer.add_table(
        text="  UKRDC", table=df_collection["ukrdc"], table_name="ukrdc"
    )
    audit_writer.add_table(
        text="  RADAR", table=df_collection["radar"], table_name="radar"
    )
    cols = df_collection["ukrdc"].columns

    source_group_id_mapping = get_source_group_id_mapping(sessions["radar"])

    # =====================< Formatting >==================

    df_collection = treatment_table_format_conversion(
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

    # TODO remove this
    # df_collection["ukrdc"] = df_collection["ukrdc"].filter(pl.col("patient_id") == 242)
    # df_collection["radar"] = df_collection["radar"].filter(pl.col("patient_id") == 242)

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
    print(reduced_dataframe)
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

    with pl.Config(tbl_cols=-1):
        print(new_treatments)
        print(existing_treatments)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TimeLineData importer script")
    # Add the arguments
    parser.add_argument("-a", "--audit", type=str, help="Audit a directory")
    parser.add_argument(
        "-c", "--commit", help="Commit to server", action="store_true", default=False
    )
    parser.add_argument(
        "-tr",
        "--test_run",
        help="run on staging servers",
        action="store_true",
        default=False,
    )
    # Parse the arguments
    args = parser.parse_args()

    # Use the arguments
    if args.audit:
        print(f"Auditing directory: {args.audit}")
        audit = AuditWriter(f"{args.audit}", "delta")
        start_time = datetime.now()
        audit.add_info("start time", str(start_time))
        main(audit_writer=audit)
        end_time = datetime.now()
        audit.add_info("end time", str(end_time))
        audit.add_info("total time", str(end_time - start_time))
        audit.commit_audit()

    else:
        main()
    if args.commit:
        print(f"Commit with verbosity level: {args.commit}")
    if args.test_run:
        print("testing")
