from rr_connection_manager import SQLServerConnection, PostgresConnection

from radar_timeline_data.utils.connections import SessionManager


def concat_cols(row):
    return str(row["uktssa_no"]) + row["transplant_type"]


def test_print_hi():
    sessions = {
        "ukrdc": SQLServerConnection(
            app="ukrdc_staging",
        ),
        "radar": SQLServerConnection(app="radar_staging"),

    }
    for i in sessions:
        session = sessions[i]
        session.connection_check()
    assert True
