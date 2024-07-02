"""
TimeLineData importer script.

This script handles the import and processing of timeline data, including treatment and transplant data.

"""

from datetime import datetime

from loguru import logger

from radar_timeline_data import (
    get_args,
    create_audit,
    calculate_runtime,
    AuditWriter,
    create_sessions,
    get_modality_codes,
    get_satellite_map,
    make_patient_map,
    get_source_group_id_mapping,
    treatment_run,
    transplant_run,
)


def main(
    audit: AuditWriter,
    commit: bool,
    test_run: bool,
) -> None:
    """
    main function for flow of script
    Args:
        audit: Object used for writing readable audit files
        commit: boolean to indicate whether to commit
        test_run: boolean to indicate whether to run on test databases
        max_data_lifetime: maximum age of data
    """

    sessions = create_sessions(test_run)
    codes = get_modality_codes(sessions["ukrdc"])
    satellite = get_satellite_map(sessions["ukrdc"])
    radar_patient_id_map = make_patient_map(sessions)
    source_group_id_mapping = get_source_group_id_mapping(sessions["radar"])

    audit.set_ws(worksheet_name="mappings")
    audit.add_table(
        text="retrieved modality codes from ukrdc",
        table=codes,
        table_name="Modality_Codes",
    )
    audit.add_table(
        text="retrieved unit codes from ukrdc",
        table=satellite,
        table_name="Satellite_Units",
    )
    audit.add_table(
        text="created a map of patients from each database",
        table=radar_patient_id_map,
        table_name="Patient_number",
    )

    treatment_run(
        audit,
        codes,
        satellite,
        sessions,
        radar_patient_id_map,
        source_group_id_mapping,
        commit,
    )

    transplant_run(audit, sessions, radar_patient_id_map, commit)

    audit.add_text("end of script")

    for session in sessions.values():
        session.close()


if __name__ == "__main__":
    args = get_args()

    logger.info("script start")
    logger.info(f"Auditing directory: {args.audit_path}")

    start_time = datetime.now()

    audit = create_audit(start_time, args.audit_path)
    audit.add_text("starting script", style="Heading 3")
    audit.add_info("time", ("start time", start_time.strftime("%Y-%m-%d %H:%M")))

    try:
        main(audit=audit, commit=args.commit, test_run=args.test_run)
    except Exception as e:
        audit.add_important("{e}", True)
        audit.commit_audit()
        raise e

    end_time = datetime.now()
    hours, minutes, seconds = calculate_runtime(end_time, start_time)

    audit.add_info("time", ("end time", end_time.strftime("%Y-%m-%d %H:%M")))
    audit.add_info(
        "time", ("total time", f"{hours} hours {minutes} mins {int(seconds)} seconds")
    )

    audit.commit_audit()

    logger.success(
        f"script finished in {hours} hours {minutes} mins {int(seconds)} seconds"
    )
