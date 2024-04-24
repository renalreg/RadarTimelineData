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
    treatment_table_format_conversion,
)


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
    # init the session connection
    # prep the dfs
    # merge dfs
    # validate dfs
    # create audit and any flags
    # commit df to radar

    audit_writer.add_text("starting script")
    # innit sessions
    sessions = create_sessions()

    # get codes from ukrdc
    codes = get_modality_codes(sessions)
    satellite = get_sattelite_map(sessions["ukrdc"])

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

    # treatment_run(audit_writer, codes, satellite, sessions, ukrdc_radar_mapping)
    rr_radar_mapping = get_rr_to_radarnumber_map(sessions)
    transplant_run(
        audit_writer, codes, satellite, sessions, ukrdc_radar_mapping, rr_radar_mapping
    )

    # send to database
    # close the sessions connection
    for session in sessions.values():
        session.session.close()


def transplant_table_format_conversion(
    df_collection: dict[str, pl.DataFrame], ukrdc_radar_mapping: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    df_collection["rr"] = (
        df_collection["rr"]
        .with_columns(
            patient_id=pl.col("pid").replace(
                ukrdc_radar_mapping.get_column("pid"),
                ukrdc_radar_mapping.get_column("patient_id"),
                default="None",
            )
        )
        .drop(["id", "pid"])
    )

    return df_collection


def transplant_run(
    audit_writer: AuditWriter | StubObject,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    sessions: dict[str, SessionManager],
    ukrdc_radar_mapping: pl.DataFrame,
    rr_radar_mapping: pl.DataFrame,
):
    print(ukrdc_radar_mapping)
    df_collection = sessions_to_transplant_dfs(
        sessions,
        ukrdc_radar_mapping.get_column("number"),
        rr_radar_mapping.get_column("number"),
    )

    # df_collection = transplant_table_format_conversion(
    #    df_collection, ukrdc_radar_mapping
    # )
    for value in df_collection:
        print(value + ":\n")
        print(df_collection[value].columns)

    print(df_collection["rr"])
    rr_radar_mapping = rr_radar_mapping.unique()
    df_collection["rr"] = df_collection["rr"].with_columns(
        RADAR_NO=pl.col("RR_NO").replace(
            rr_radar_mapping.get_column("number"),
            rr_radar_mapping.get_column("patient_id"),
            default="None",
        )
    )
    with pl.Config(tbl_cols=-1):
        print(df_collection["rr"])

    pass


def treatment_run(audit_writer, codes, satellite, sessions, ukrdc_radar_mapping):
    # from all sessions get treatment tables
    df_collection = sessions_to_treatment_dfs(
        sessions, ukrdc_radar_mapping.get_column("number")
    )
    # replace id with id_str to use later
    df_collection["radar"] = df_collection["radar"].drop("id").rename({"id_str": "id"})
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
    print(source_group_id_mapping)
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
    # TODO remove this
    df_collection["ukrdc"] = df_collection["ukrdc"].filter(pl.col("patient_id") == 242)
    df_collection["radar"] = df_collection["radar"].filter(pl.col("patient_id") == 242)
    print(df_collection["ukrdc"])
    # sort by patient id, modality and from date
    audit_writer.set_ws("group_reduce_Treatment")
    df_collection["ukrdc"] = group_and_reduce_ukrdc_dataframe(
        df_collection, audit_writer
    )
    # combine all dataframes into one
    audit_writer.set_ws("raw_all_Treatment")
    print(df_collection)
    combined_dataframe = combine_treatment_dataframes(df_collection)
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
    # reduce the dataframe
    reduced_dataframe = group_and_reduce_combined_dataframe(combined_dataframe)
    audit_writer.add_table(
        "reducing_combined_Treatment",
        reduced_dataframe,
        table_name="reduced_combined_Treatment",
    )
    # new rows
    print()
    existing_treatments, new_treatments = split_combined_dataframe(
        combined_dataframe, reduced_dataframe
    )
    # clean up
    del combined_dataframe, reduced_dataframe
    audit_writer.set_ws("Treatment_output")
    new_treatments, existing_treatments = fill_null_time(
        new_treatments, existing_treatments
    )
    audit_writer.add_table(
        text="data that is new", table=new_treatments, table_name="new_Treatment"
    )
    audit_writer.add_table(
        text="data to update", table=existing_treatments, table_name="update_Treatment"
    )
    with pl.Config(tbl_cols=-1, tbl_rows=-1):
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
