import argparse
import polars as pl
from datetime import datetime

from radar_timeline_data import AuditWriter


def get_args():
    parser = argparse.ArgumentParser(description="TimeLineData importer script")
    # Add the arguments
    parser.add_argument(
        "-ap",
        "--audit_path",
        required=True,
        type=str,
        help="Directory to store the audit files",
    )
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
    return parser.parse_args()


def calculate_runtime(end_time, start_time):
    total_seconds = (end_time - start_time).total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return hours, minutes, seconds


def create_audit(start_time, audit_path):
    audit: AuditWriter = AuditWriter(
        f"{audit_path}",
        f"rdrTimeLineDataLog-{start_time.strftime('%d-%m-%Y')}",
        "Radar Timeline Data Log",
        include_excel=True,
        include_breakdown=True,
    )

    return audit


def check_nulls_in_column(df, col):
    if df[col].is_null().any():
        raise ValueError(f"Column {col} contains null values.")


def max_with_nulls(column: pl.Expr) -> pl.Expr:
    return column.sort(descending=True, nulls_last=False).first()


def fill_null_time(added_rows, update_rows) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Fills null values in 'modified_date' and 'created_date' columns of the input DataFrames
    with the current datetime.

    Args:
    - added_rows (pl.DataFrame): DataFrame containing rows that were added
    - update_rows (pl.DataFrame): DataFrame containing rows that were updated

    Returns:
    tuple[pl.DataFrame, pl.DataFrame]: Tuple of DataFrames with null values filled in 'modified_date'
    and 'created_date' columns using the current datetime.
    """
    time = datetime.now()
    added_rows = added_rows.with_columns(
        modified_date=pl.col("modified_date").fill_null(time),
        created_date=pl.col("created_date").fill_null(time),
    )
    update_rows = update_rows.with_columns(
        modified_date=pl.col("modified_date").fill_null(time),
        created_date=pl.col("created_date").fill_null(time),
    )
    return added_rows, update_rows


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]
