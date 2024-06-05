import polars as pl
from radar_models import radar2 as radar
from sqlalchemy import select
from sqlalchemy.orm import Session

from radar_timeline_data.utils.connections import get_data_as_df


def get_rr_transplant_modality(rr_df: pl.DataFrame) -> pl.DataFrame:
    """
    Get the transplant modality based on specific conditions.

    Args:
        rr_df: pl.DataFrame - A Polars DataFrame containing transplant data.

    Returns:
        pl.DataFrame: A Polars DataFrame with an added column 'modality' representing the transplant modality.

    Examples:
        >>> df = pl.DataFrame({
        ...     "TRANSPLANT_TYPE": ["Live", "DCD", "Live"],
        ...     "TRANSPLANT_RELATIONSHIP": ["0", "2", "9"],
        ...     "TRANSPLANT_SEX": ["1", "2", "1"]
        ... })
        >>> result = get_rr_transplant_modality(df)
    """

    ttype = pl.col("transplant_type")
    alive = ttype.is_in(["Live"])
    dead = ttype.is_in(["DCD", "DBD"])
    trel = pl.col("transplant_relationship")
    tsex = pl.col("transplant_sex")
    father = "1"
    mother = "2"
    # TODO missing 25 to 28
    rr_df = rr_df.with_columns(
        # child
        pl.when(alive & (trel == "0"))
        .then(77)
        # sibling
        .when(alive & (trel.is_in(["3", "4", "5", "6", "7", "8"])))
        .then(21)
        # father
        .when(alive & (trel == "2") & (tsex == father))
        .then(74)
        # mother
        .when(alive & (trel == "2") & (tsex == mother))
        .then(75)
        # other related
        .when(alive & (trel == "9"))
        .then(23)
        # live unrelated
        .when(alive & (trel.is_in(["11", "12", "15", "16", "19", "10"])))
        .then(24)
        # cadaver donor
        .when(dead)
        .then(20)
        # unknown
        .when(trel.is_in(["88", "99"]))
        .then(99)
        .otherwise(None)
        .alias("modality")
    ).cast({"modality": pl.Int64})
    return rr_df


def convert_transplant_unit(df_collection, sessions: dict[str, Session]):
    """
    Converts transplant unit codes in a DataFrame using a mapping obtained from a database session.

    Args:
        df_collection: dict - A dictionary containing DataFrames, where 'rr' DataFrame has 'TRANSPLANT_UNIT' column.
        sessions: dict - A dictionary of database sessions, with 'radar' key used to query mapping data.

    Returns:
        dict: A dictionary with updated 'rr' DataFrame containing mapped 'TRANSPLANT_UNIT' values.

    Raises:
        KeyError: If the 'TRANSPLANT_UNIT' column is missing in the 'rr' DataFrame.
    """

    query = select(radar.Group.id, radar.Group.code).filter(
        radar.Group.type == "HOSPITAL"
    )
    kmap = get_data_as_df(sessions["radar"], query)
    df_collection["rr"] = df_collection["rr"].with_columns(
        TRANSPLANT_UNIT=pl.col("TRANSPLANT_UNIT").replace(
            kmap.get_column("code"),
            kmap.get_column("id"),
            default=None,
        )
    )
    return df_collection
