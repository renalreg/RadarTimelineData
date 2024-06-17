"""
TimeLineData importer script.

This script handles the import and processing of timeline data, including treatment and transplant data.

"""

from datetime import datetime
from typing import Any

from loguru import logger

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.runs.transplant_run import transplant_run
from radar_timeline_data.runs.treatment_run import treatment_run
from radar_timeline_data.utils.args import get_args
from radar_timeline_data.utils.connections import (
    create_sessions,
    get_modality_codes,
    get_sattelite_map,
)
from radar_timeline_data.utils.patient_map import make_patient_map


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
        commit: boolean to indicate whether to commit
        test_run: boolean to indicate whether to run on test databases
        max_data_lifetime: maximum age of data
    """

    # =======================< START >====================

    audit_writer.add_text("starting script", style="Heading 3")
    sessions = create_sessions()

    codes = get_modality_codes(sessions["ukrdc"])
    satellite = get_sattelite_map(sessions["ukrdc"])

    radar_patient_id_map = make_patient_map(sessions)
    # write tables to audit
    audit_writer.set_ws(worksheet_name="mappings")
    audit_writer.add_table(
        text="retrieved modality codes from ukrdc",
        table=codes,
        table_name="Modality_Codes",
    )
    audit_writer.add_table(
        text="retrieved unit codes from ukrdc",
        table=satellite,
        table_name="Satellite_Units",
    )

    audit_writer.add_table(
        text="created a map of patients from each database",
        table=radar_patient_id_map,
        table_name="Patient_number",
    )

    # =======================< TRANSPLANT AND TREATMENT RUNS >====================
    audit_writer.add_text("Treatment Process", "Heading 3")
    treatment_run(
        audit_writer, codes, satellite, sessions, radar_patient_id_map, commit
    )
    del codes

    audit_writer.add_text("Transplant Process", "Heading 3")

    transplant_run(audit_writer, sessions, radar_patient_id_map)

    audit_writer.add_text("end of script")

    # send to database
    # close the sessions connection
    for session in sessions.values():
        session.close()


if __name__ == "__main__":
    logger.info("script start")
    args = get_args()

    start_time = datetime.now()
    audit: AuditWriter | StubObject = (
        AuditWriter(
            f"{args.audit}",
            f"rdrTimeLineDataLog-{start_time.strftime('%d-%m-%Y')}",
            "Radar Timeline Data Log",
            include_excel=True,
            include_breakdown=True,
        )
        if args.audit
        else StubObject()
    )
    params: dict[str, Any]
    params = {"audit_writer": audit}
    if args.commit:
        params["commit"] = args.commit
    if args.test_run:
        params["test_run"] = args.test_run

    if args.audit:
        logger.info(f"Auditing directory: {args.audit}")

    # Recording start time

    audit.add_info("time", ("start time", start_time.strftime("%Y-%m-%d %H:%M")))

    # Calling main function
    try:
        main(**params)
    except Exception as e:
        audit.add_important("{e}", True)
        audit.commit_audit()
        raise e

    # Recording end time
    end_time = datetime.now()
    audit.add_info("time", ("end time", end_time.strftime("%Y-%m-%d %H:%M")))

    # Calculating and recording total time
    total_seconds = (end_time - start_time).total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    audit.add_info(
        "time", ("total time", f"{hours} hours {minutes} mins {int(seconds)} seconds")
    )
    audit.commit_audit()

    # Logging script completion
    logger.success(
        f"script finished in {hours} hours {minutes} mins {int(seconds)} seconds"
    )
