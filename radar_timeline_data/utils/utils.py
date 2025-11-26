import argparse
from datetime import datetime

import polars as pl
from sqlalchemy import (
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    inspect,
)

from radar_timeline_data.utils.config import override_dict


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


def sqla_to_polars_schema(models):
    """
    Generates a polars schema from SQLAlchemy models.

    Parameters:
    models (list): List of SQLAlchemy ORM models to include in the schema.

    Returns:
        dict: A dictionary with column names as keys and polars data types as values.
    """
    SQLALCHEMY_TO_POLARS_TYPE = {
        Integer: pl.Int32,
        Float: pl.Float64,
        String: pl.Utf8,
        DateTime: pl.Datetime,
        Date: pl.Date,
    }
    schema = {}

    for model in models:
        mapper = inspect(model)

        for column in mapper.columns:
            col_name = column.name
            col_type = type(column.type)

            # Check for Numeric type with precision and scale
            if isinstance(column.type, Numeric):
                # Extract precision and scale from Numeric type
                precision, scale = column.type.precision, column.type.scale
                if precision and scale is not None:
                    # Map to pl.Decimal with the same precision and scale
                    pl_type = pl.Decimal(precision, scale)
                else:
                    # Default to pl.Float64 if no precision/scale is specified
                    pl_type = pl.Float64
            else:
                # Use the default mapping for other types
                pl_type = SQLALCHEMY_TO_POLARS_TYPE.get(col_type, pl.Utf8)

            # Add column name and type to the schema
            schema[col_name] = pl_type

    overrides = override_dict
    schema = {
        **schema,  # original types
        **overrides,  # override types (added or replaced)
        **{
            k.upper(): v for k, v in overrides.items()
        },  # where key is in capitals instead
    }

    return schema
