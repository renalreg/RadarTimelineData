from datetime import datetime
from typing import List

import polars as pl

from radar_timeline_data.audit_writer.audit_writer import AuditWriter, StubObject
from radar_timeline_data.utils.polar_utils import max_with_nulls


def column_name_and_type_change(
    df_collection: dict[str, pl.DataFrame]
) -> dict[str, pl.DataFrame]:
    """
    Modify column names and types for the 'ukrdc' DataFrame within the collection.

    Args:
    - df_collection (dict[str, pl.DataFrame]): A dictionary containing DataFrames, with 'ukrdc' as one of the keys.

    Returns:
    - dict[str, pl.DataFrame]: The modified DataFrame collection with updated column names and types.
    """
    df_collection["ukrdc"] = df_collection["ukrdc"].rename(
        {
            "creation_date": "created_date",
            "update_date": "modified_date",
            "fromtime": "from_date",
            "totime": "to_date",
            "admitreasoncode": "modality",
        },
    )
    df_collection["ukrdc"] = df_collection["ukrdc"].cast(
        {"to_date": pl.Date, "from_date": pl.Date, "patient_id": pl.Int64}
    )
    df_collection["ukrdc"] = df_collection["ukrdc"].cast(
        {"source_group_id": pl.Int64}, strict=False
    )
    return df_collection


def group_and_reduce_ukrdc_dataframe(
    df_collection: dict[str, pl.DataFrame],
    audit_writer: AuditWriter | StubObject = StubObject(),
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
        "grouping similar or overlapping date of subsection patient_id and modality",
        df_collection["ukrdc"],
        "date_range_over_patient_id_modality_ukrdc",
    )
    # TODO ask david about this part
    # get min and max dates for recent date to get most up to date row
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        pl.max_horizontal(["created_date", "modified_date"]).alias("recent_date")
    )
    df_collection["ukrdc"] = (
        df_collection["ukrdc"]
        .sort(
            "recent_date",
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
        "reduce ukrdc by grouped ranges",
        df_collection["ukrdc"],
        "date_range_over_patient_id_modality_reduced_ukrdc",
    )

    # with the now grouped ranges check that no treatments overlap with another
    # set to 15 days as patient can not undergo diffrent treatment within 15 days
    df_collection["ukrdc"] = group_similar_or_overlapping_range(
        df_collection["ukrdc"], ["patient_id"], day_overide=15
    )

    audit_writer.add_table(
        "grouping similiar or overlapping date ranges of subsection patient_id to check that no treatments overlap",
        df_collection["ukrdc"],
        "date_range_over_patient_id_ukrdc",
    )

    # TODO also ask david about this part
    # aggregate columns into one
    df_collection["ukrdc"] = (
        df_collection["ukrdc"]
        .sort(
            "recent_date",
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
        .drop(columns=["group_id", "id", "recent_date"])
    )

    audit_writer.add_table(
        "reduce",
        df_collection["ukrdc"],
        "date_range_over_patient_id_reduced_ukrdc",
    )

    return df_collection["ukrdc"]


def combine_treatment_dataframes(
    df_collection: dict[str, pl.DataFrame]
) -> pl.DataFrame:
    """
    Combines multiple dataframes into one, handling missing columns by filling nulls diagonally.
    Encodes source types to numerical values based on their priority.
    Overwrites radar data with ukrdc data.
    Groups similar or overlapping date ranges within the combined dataframe.

    Parameters:
    - df_collection (dict): A dictionary containing dataframes with keys "ukrdc" and "radar". must contain
    source_type column, from_date and to_date

    Returns:
    - pl.DataFrame: Combined dataframe with processed data.
    """

    # combine dataframes into 1,diagonal = missing cols become nulls
    combined_dataframe = pl.concat(
        [df_collection[i] for i in df_collection], how="diagonal_relaxed"
    ).sort("patient_id", "modality", "from_date")
    combined_dataframe = combined_dataframe.with_columns(
        pl.max_horizontal(["created_date", "modified_date"]).alias("recent_date")
    )
    # encode source types to numerical value based on there priority
    combined_dataframe = combined_dataframe.with_columns(
        source_type=pl.col("source_type")
        .replace(
            old=["BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3"],
            default=None,
        )
        .cast(pl.Int32)
    )
    # overwrite radar with ukrdc
    combined_dataframe = combined_dataframe.sort(
        ["patient_id", "source_type", "recent_date", "from_date"],
        descending=True,
    )
    combined_dataframe = group_similar_or_overlapping_range(
        combined_dataframe, ["patient_id"], 15
    )
    return combined_dataframe


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
                "created_user_id",
                "created_date",
                "modified_user_id",
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
                "created_user_id",
                "created_date",
                "modified_user_id",
                "modified_date",
                "recent_date",
            ]
        )
    )


def group_similar_or_overlapping_range(
    df: pl.DataFrame, window: List[str], day_overide: int = 5
) -> pl.DataFrame:
    """
    Group similar or overlapping date ranges within specified window.

    Args:
        df (pl.DataFrame): Input DataFrame containing date ranges.
        window (List[str]): List of column names to partition the data.
        day_overide (int): How many days either side or range to class as overlapping

    Returns:
        pl.DataFrame: DataFrame with added 'group_id' column indicating groupings of similar or overlapping ranges.
    """

    # Sort by patient_id, modality, and from_date, with to_date descending as null values are considered the highest
    if df["from_date"].is_null().any():
        raise ValueError("Column 'from_date' contains null values.")

    df = df.sort(
        window + ["from_date", "to_date"],
        descending=([False] * len(window) + [False, True]),
    ).with_columns(pl.col("from_date").shift().over(window).alias("prev_from_date"))
    # Sort by patient_id, modality, and to_date where nulls are considered to be the largest values/ end of date range
    # apply forward fills to non starting null values in each group
    df = df.sort(window + ["to_date"], nulls_last=True).with_columns(
        pl.col("to_date").shift().forward_fill().over(window).alias("prev_to_date")
    )
    # Define a mask for overlapping dates or dates within 5 days
    mask = overlapping_dates_bool_mask(days=day_overide)
    df = (
        df.sort(
            window + ["from_date", "to_date"],
            descending=([False] * len(window) + [False, True]),
        )
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
    mask = (
        (
            (pl.col("from_date") <= pl.col("prev_to_date"))
            & (pl.col("from_date") >= pl.col("prev_from_date"))
        )
        | (
            (pl.col("to_date") <= pl.col("prev_to_date"))
            & (pl.col("to_date") >= pl.col("prev_from_date"))
        )
        | (abs(pl.col("to_date") - pl.col("prev_from_date")) <= pl.duration(days=days))
        | (abs(pl.col("from_date") - pl.col("prev_to_date")) <= pl.duration(days=days))
        | (
            abs(pl.col("from_date") - pl.col("prev_from_date"))
            <= pl.duration(days=days)
        )
        | (abs(pl.col("to_date") - pl.col("prev_to_date")) <= pl.duration(days=days))
    )
    return mask


def format_treatment(
    codes: pl.DataFrame,
    df_collection: dict[str, pl.DataFrame],
    satellite: pl.DataFrame,
    source_group_id_mapping: pl.DataFrame,
    radar_patient_id_map: pl.DataFrame,
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
    pat_map = radar_patient_id_map.drop_nulls(["ukrdcid"]).unique(subset=["ukrdcid"])

    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        patient_id=pl.col("ukrdcid").replace(
            pat_map.get_column("ukrdcid"),
            pat_map.get_column("radar_id"),
            default="None",
        )
    )

    # TODO df_collection["ukrdc"].filter(pl.col("patient_id").is_null())

    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        healthcarefacilitycode=pl.col("healthcarefacilitycode").replace(
            satellite.get_column("satellite_code"),
            satellite.get_column("main_unit_code"),
        )
    )
    # replace main unite code with number equivalent and set to source group id
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        source_group_id=pl.col("healthcarefacilitycode").replace(
            source_group_id_mapping.get_column("code"),
            source_group_id_mapping.get_column("id"),
        )
    )
    # format codes to radar std rr7 41 -> 1
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        admitreasoncode=pl.col("admitreasoncode").replace(
            codes.get_column("registry_code"),
            codes.get_column("equiv_modality"),
            default=None,
        )
    )
    df_collection = column_name_and_type_change(df_collection)
    # TODO see if there is a source type column available through a join so that a each source type can be queried
    #  differently
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        source_type=pl.lit("UKRDC")
    )
    # TODO change the below
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        created_user_id=pl.lit(None)
    )
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        modified_user_id=pl.lit(None)
    )

    df_collection["ukrdc"] = df_collection["ukrdc"].select(
        [
            "id",
            "patient_id",
            "source_group_id",
            "source_type",
            "from_date",
            "to_date",
            "modality",
            "created_user_id",
            "created_date",
            "modified_user_id",
            "modified_date",
        ]
    )
    df_collection["ukrdc"] = df_collection["ukrdc"].cast(
        {
            "modified_user_id": pl.Int64,
            "created_user_id": pl.Int64,
            "modality": pl.Int64,
        }
    )
    return df_collection
