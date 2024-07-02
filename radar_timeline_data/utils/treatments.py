import polars as pl
import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc as ukrdc
from sqlalchemy.orm import Session
from sqlalchemy import String, Date, cast
from typing import List

from radar_timeline_data.audit_writer.audit_writer import AuditWriter
from radar_timeline_data.utils.connections import (
    df_batch_insert_to_sql,
    get_data_as_df,
)

from radar_timeline_data.utils.utils import (
    fill_null_time,
    check_nulls_in_column,
    max_with_nulls,
)


def treatment_run(
    audit_writer: AuditWriter,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    sessions: dict[str, Session],
    radar_patient_id_map: pl.DataFrame,
    source_group_id_mapping: pl.DataFrame,
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

    df_collection = make_treatment_dfs(
        sessions,
        radar_patient_id_map.filter(pl.col("ukrdcid").is_not_null()).get_column(
            "ukrdcid"
        ),
        codes,
        satellite,
        source_group_id_mapping,
        radar_patient_id_map,
        audit_writer,
    )

    df_collection = format_treatment(
        codes,
        df_collection,
        satellite,
        source_group_id_mapping,
        radar_patient_id_map,
        audit_writer,
    )

    cols = df_collection["ukrdc"].head()

    audit_writer.add_text("Treatment Process", "Heading 3")
    audit_writer.add_text("Importing Treatment data from:")
    audit_writer.set_ws(worksheet_name="treatment_import")
    audit_writer.add_table(
        text="  UKRDC", table=df_collection["ukrdc"], table_name="treatment_ukrdc"
    )
    audit_writer.add_table(
        text="  RADAR", table=df_collection["radar"], table_name="treatment_radar"
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
    audit_writer.add_text(
        "After formatting treatments in radar format similar treatments need to be Aggregated"
    )
    audit_writer.set_ws("group_reduce_Treatment")

    df_collection = group_and_reduce_ukrdc_dataframe(df_collection, audit_writer)

    combined_dataframe = combine_treatment_dataframes(df_collection)

    audit_writer.set_ws("raw_all_Treatment")
    audit_writer.add_table(
        text="Combine data from UKRDC and RADAR",
        table=combined_dataframe,
        table_name="raw_combined_Treatment",
    )

    audit_writer.set_ws("group_reduce_all_Treatment")
    audit_writer.add_text(
        "The data is now consolidated into one table and requires grouping and aggregation."
    )

    reduced_dataframe = group_and_reduce_combined_treatment_dataframe(
        combined_dataframe
    )

    audit_writer.add_table(
        "All treatments have been grouped and reduced",
        reduced_dataframe,
        table_name="reduced_combined_Treatment",
    )

    existing_treatments, new_treatments = split_combined_dataframe(
        combined_dataframe, reduced_dataframe
    )

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
            new_treatments, sessions["radar"], radar.Dialysi.__table__, 1000, "id"
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


def format_treatment(
    codes: pl.DataFrame,
    df_collection: dict[str, pl.DataFrame],
    satellite: pl.DataFrame,
    source_group_id_mapping: pl.DataFrame,
    radar_patient_id_map: pl.DataFrame,
    audit_writer: AuditWriter,
) -> dict[str, pl.DataFrame]:
    """
    Convert data format for UKRDC treatment table.

    Args:
        codes (pl.DataFrame): DataFrame containing codes for mapping.
        df_collection (dict[str, pl.DataFrame]): Dictionary of DataFrames with keys as DataFrame names.
        satellite (pl.DataFrame): DataFrame containing satellite information.
        source_group_id_mapping (pl.DataFrame): DataFrame containing source group ID mapping.
        ukrdc_radar_mapping (pl.DataFrame): DataFrame containing UKRDC radar mapping.

    Returns:
        dict[str, pl.DataFrame]: Dictionary of DataFrames with updated UKRDC treatment table format.
    """

    audit_writer.add_text("formating ukrdc data into radar format")
    pat_map = radar_patient_id_map.drop_nulls(["ukrdcid"]).unique(subset=["ukrdcid"])

    audit_writer.add_change(
        "using map of patients convert ukrdc ids into radar ids and assign to patient_id column",
        [
            ["ukrdcid"],
            ["radar ids"],
            ["patient_id"],
        ],
    )

    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        id=pl.lit(None),
        source_type=pl.lit(None),
        source_group_id=pl.col("source_group_id").replace(
            satellite.get_column("satellite_code"),
            satellite.get_column("main_unit_code"),
        ),
    )

    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        patient_id=pl.col("patient_id").replace(
            pat_map.get_column("ukrdcid"),
            pat_map.get_column("radar_id"),
            default="None",
        ),
        source_group_id=pl.col("source_group_id").replace(
            source_group_id_mapping.get_column("code"),
            source_group_id_mapping.get_column("id"),
        ),
        modality=pl.col("modality").replace(
            codes.get_column("registry_code"),
            codes.get_column("equiv_modality"),
            default=None,
        ),
    )

    return df_collection


def make_treatment_dfs(
    sessions: dict[str, Session],
    filter: pl.Series,
    codes: pl.DataFrame,
    satellite: pl.DataFrame,
    source_group_id_mapping: pl.DataFrame,
    radar_patient_id_map: pl.DataFrame,
    audit_writer: AuditWriter,
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding treatments.

    Args:
        sessions (dict): A dictionary containing session information.
        filter (pl.Series, optional):A filter of ids to pull

    Returns:
        dict: A dictionary containing DataFrames corresponding to each session.
    """

    # Cast to str because of issues with Polars and UUID's
    radar_query = (
        sessions["radar"]
        .query(
            cast(radar.Dialysi.id, String),
            cast(radar.Dialysi.patient_id, String),
            cast(radar.Dialysi.source_group_id, String),
            radar.Dialysi.source_type,
            radar.Dialysi.from_date,
            radar.Dialysi.to_date,
            cast(radar.Dialysi.modality, String),
            cast(radar.Dialysi.created_date, Date),
            cast(radar.Dialysi.modified_date, Date),
        )
        .statement
    )

    df_collection = {"radar": get_data_as_df(sessions["radar"], radar_query)}

    check_nulls_in_column(df_collection["radar"], "from_date")

    str_filter = filter.cast(pl.String).to_list()

    ukrdc_query = (
        sessions["ukrdc"]
        .query(
            ukrdc.Treatment.id,
            ukrdc.PatientRecord.ukrdcid.label("patient_id"),
            ukrdc.Treatment.healthcarefacilitycode.label("source_group_id"),
            cast(ukrdc.Treatment.fromtime, Date).label("from_date"),
            cast(ukrdc.Treatment.totime, Date).label("to_date"),
            ukrdc.Treatment.admitreasoncode.label("modality"),
            ukrdc.Treatment.creation_date.label("created_date"),
            ukrdc.Treatment.update_date.label("modified_date"),
        )
        .join(ukrdc.PatientRecord, ukrdc.Treatment.pid == ukrdc.PatientRecord.pid)
        .filter(ukrdc.PatientRecord.ukrdcid.in_(str_filter))
        .statement
    )

    df_collection["ukrdc"] = get_data_as_df(sessions["ukrdc"], ukrdc_query)
    check_nulls_in_column(df_collection["ukrdc"], "from_date")

    return df_collection


def group_and_reduce_ukrdc_dataframe(
    df_collection: dict[str, pl.DataFrame],
    audit_writer: AuditWriter,
) -> pl.DataFrame:
    """
    Group and reduce the combined DataFrame by patient_id and group_id.
    The resulting DataFrame contains the first occurrence of each column for each patient-group combination.

    Args:
    - combined_dataframe (DataFrame): The input DataFrame containing combined data.

    Returns:
    - DataFrame: The reduced DataFrame with grouped and aggregated data.
    """

    df_collection["ukrdc"] = group_similar_or_overlapping_range(
        df_collection["ukrdc"], ["patient_id", "modality"]
    )

    audit_writer.add_table(
        "Grouping treatments by modality and patient ID, "
        "where each treatment within a group overlaps or is within 5 days of another",
        df_collection["ukrdc"],
        "date_range_over_patient_id_modality_ukrdc",
    )

    # TODO ask david about this part may not be needed as modality grouping will cover this
    # get min and max dates for recent date to get most up to date row
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        pl.max_horizontal(["created_date", "modified_date"]).alias("most_recent_date")
    )

    # TODO: Explain
    # Group patient_id, modality, and group_id getting the earliest and latest to and from date?
    # does this account for re-occurring modalities
    # |---- MOD A ----||---- MOD B ----||---- MOD A ----|
    # Would this become
    # |---- MOD A --------------------------------------|
    #                  |----MOD B ----|
    df_collection["ukrdc"] = (
        df_collection["ukrdc"]
        .sort(
            "most_recent_date",
            descending=True,
        )
        .group_by(["patient_id", "modality", "group_id"])
        .agg(
            pl.col("from_date").min(),
            max_with_nulls(pl.col("to_date")).alias("to_date"),
            **{
                col: pl.col(col).first()
                for col in df_collection["ukrdc"].columns
                if col
                not in ["from_date", "to_date", "patient_id", "modality", "group_id"]
            },
        )
    )

    audit_writer.add_table(
        "Reducing treatments by selecting representative values from each group",
        df_collection["ukrdc"],
        "date_range_over_patient_id_modality_reduced_ukrdc",
    )

    # with the now grouped ranges check that no treatments overlap with another
    # set to 15 days as patient can not undergo different treatment within 15 days
    df_collection["ukrdc"] = group_similar_or_overlapping_range(
        df_collection["ukrdc"], ["patient_id"], day_override=15
    )

    audit_writer.add_table(
        "Re Grouping treatments by patient ID, "
        "where each treatment within a group overlaps or is within 15 days of another effectively combining modalities",
        df_collection["ukrdc"],
        "date_range_over_patient_id_ukrdc",
    )

    # TODO also ask david about this part
    # aggregate columns into one
    df_collection["ukrdc"] = (
        df_collection["ukrdc"]
        .sort(
            "most_recent_date",
            descending=True,
        )
        .group_by(["patient_id", "group_id"])
        .agg(
            pl.col("from_date").min(),
            max_with_nulls(pl.col("to_date")).alias("to_date"),
            pl.col("modality").first(),
            **{
                col: pl.col(col).first()
                for col in df_collection["ukrdc"].columns
                if col
                not in ["from_date", "to_date", "patient_id", "modality", "group_id"]
            },
        )
        .drop(columns=["group_id", "id", "most_recent_date"])
    )

    audit_writer.add_table(
        "Reducing treatments by selecting representative values from each group",
        df_collection["ukrdc"],
        "date_range_over_patient_id_reduced_ukrdc",
    )

    return df_collection


def group_similar_or_overlapping_range(
    df: pl.DataFrame, window: List[str], day_override: int = 5
) -> pl.DataFrame:
    """
    Group similar or overlapping date ranges within a specified window.
    TODO: Explain why

    Args:
        df (pl.DataFrame): Input DataFrame containing date ranges.
        window (List[str]): List of column names to partition the data.
        day_override (int): Number of days to consider ranges as overlapping.

    Returns:
        pl.DataFrame: DataFrame with 'group_id' column indicating groupings of similar or overlapping ranges.
    """

    mask = overlapping_dates_bool_mask(days=day_override)
    descending = [False] * len(window) + [False, True]

    # TODO: Why?
    df = df.sort(window + ["from_date", "to_date"], descending=descending).with_columns(
        pl.col("from_date").shift().over(window).alias("prev_from_date")
    )

    # TODO: Why?
    df = df.sort(window + ["to_date"], nulls_last=True).with_columns(
        pl.col("to_date").shift().forward_fill().over(window).alias("prev_to_date")
    )

    # TODO: Why?
    df = (
        df.sort(window + ["from_date", "to_date"], descending=descending)
        .with_columns(pl.when(mask).then(0).otherwise(1).over(window).alias("group_id"))
        .with_columns(
            pl.col("group_id").cum_sum().rle_id().over(window).alias("group_id")
        )
    )

    return df.drop(["prev_to_date", "prev_from_date"])


def overlapping_dates_bool_mask(days: int = 5):
    """
    Generates a boolean mask to identify overlapping date ranges.

    Parameters:
    - days (int): Number of days within which date ranges are considered overlapping.

    Returns:
    - mask (boolean): A boolean mask indicating overlapping date ranges.
    """

    from_date = pl.col("from_date")
    to_date = pl.col("to_date")
    prev_from_date = pl.col("prev_from_date")
    prev_to_date = pl.col("prev_to_date")
    days_duration = pl.duration(days=days)

    overlap1 = (from_date <= prev_to_date) & (from_date >= prev_from_date)
    overlap2 = (to_date <= prev_to_date) & (to_date >= prev_from_date)

    date_diff1 = abs(to_date - prev_from_date) <= days_duration
    date_diff2 = abs(from_date - prev_to_date) <= days_duration
    date_diff3 = abs(from_date - prev_from_date) <= days_duration
    date_diff4 = abs(to_date - prev_to_date) <= days_duration

    return overlap1 | overlap2 | date_diff1 | date_diff2 | date_diff3 | date_diff4


def combine_treatment_dataframes(
    df_collection: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """
    Combines multiple dataframes into one, handling missing columns by filling nulls diagonally.
    Encodes source types to numerical values based on their priority.
    Overwrites radar data with ukrdc data.
    Groups similar or overlapping date ranges within the combined dataframe.

    Parameters:
    - df_collection (dict): A dictionary containing dataframes with keys "ukrdc" and "radar". Must contain
      source_type column, from_date, and to_date.

    Returns:
    - pl.DataFrame: Combined dataframe with processed data.
    """

    # Combine dataframes into one, handling missing columns by filling with nulls
    combined_dataframe = pl.concat(
        list(df_collection.values()), how="diagonal_relaxed"
    ).sort(["patient_id", "modality", "from_date"])

    combined_dataframe = combined_dataframe.with_columns(
        pl.max_horizontal(["created_date", "modified_date"]).alias("recent_date")
    )

    # Encode source types to numerical values based on their priority
    combined_dataframe = combined_dataframe.with_columns(
        pl.col("source_type")
        .replace(
            old=["BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3"],
            default=None,
        )
        .cast(pl.Int32)
    )

    combined_dataframe = combined_dataframe.sort(
        ["patient_id", "source_type", "recent_date", "from_date"], descending=True
    )

    combined_dataframe = group_similar_or_overlapping_range(
        combined_dataframe, ["patient_id"], 15
    )

    return combined_dataframe


def group_and_reduce_combined_treatment_dataframe(reduced_dataframe: pl.DataFrame):
    """
    Group and reduce the combined DataFrame by patient_id and group_id.
    The resulting DataFrame contains the first occurrence of each column for each patient-group combination.

    Args:
    - combined_dataframe (DataFrame): The input DataFrame containing combined data.

    Returns:
    - DataFrame: The reduced DataFrame with grouped and aggregated data.

    Description:
    This function sorts the input DataFrame by patient_id, source_type, recent_date, and from_date in descending order.
    It then groups the sorted DataFrame by patient_id and group_id, and aggregates the data by selecting the first non-null value for each column.
    The 'group_id' column is dropped from the DataFrame, and the 'source_type' column is cast to string and replaced with corresponding labels.
    Finally, a subset of columns is selected and returned as the reduced DataFrame.
    """
    return (
        reduced_dataframe.sort(
            ["patient_id", "source_type", "recent_date", "from_date"],
            descending=True,
        )
        .group_by(["patient_id", "group_id"])
        .agg(
            pl.col("id").filter(pl.col("id").is_not_null()).first(),
            **{
                col: pl.col(col).first()
                for col in reduced_dataframe.columns
                if col not in ["id", "patient_id", "group_id"]
            },
        )
        .drop("group_id")
        .with_columns(
            source_type=pl.col("source_type")
            .cast(pl.String)
            .replace(
                new=["BATCH", "UKRDC", "RADAR", "RR"],
                old=["0", "1", "2", "3"],
                default=None,
            )
        )
        .select(
            [
                "patient_id",
                "id",
                "from_date",
                "to_date",
                "modality",
                "source_group_id",
                "source_type",
                "created_date",
                "modified_date",
                "recent_date",
            ]
        )
    )


def split_combined_dataframe(
    full_dataframe: pl.DataFrame, reduced_dataframe: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Splits a combined DataFrame into two separate DataFrames (new , existing) based on the presence of 'id' values.

    Args: full_dataframe (pl.DataFrame): The combined DataFrame containing all data. reduced_dataframe (
    pl.DataFrame): The DataFrame to be split, should be a result of full dataframe manipulations, potentially
    containing newly added rows or updates.

    Returns:
    existing_rows (DataFrame): DataFrame containing rows from 'dataframe' that have corresponding 'id' values in 'full_dataframe'.
    new_rows (DataFrame): DataFrame containing rows from 'dataframe' with 'id' values that are not present in 'full_dataframe' (null ids).
    """
    new_rows = reduced_dataframe.filter(pl.col("id").is_null())
    existing_rows = reduced_dataframe.filter(pl.col("id").is_not_null()).with_columns(
        full_dataframe.select(
            [
                "patient_id",
                "id",
                "from_date",
                "to_date",
                "modality",
                "source_group_id",
                "source_type",
                "created_date",
                "modified_date",
                "recent_date",
            ]
        )
        .filter(pl.col("id").is_in(reduced_dataframe["id"].to_list()))
        .with_columns(
            source_type=pl.col("source_type")
            .cast(pl.String)
            .replace(
                new=["BATCH", "UKRDC", "RADAR", "RR"],
                old=["0", "1", "2", "3"],
                default=None,
            )
        )
    )
    # TODO check if filter is required
    return existing_rows, new_rows
