import datetime
import decimal
from _operator import or_
from functools import reduce
from typing import List, Optional

import polars as pl
import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc as ukrdc
from sqlalchemy import (
    String,
    Date,
    cast,
    Column,
    PrimaryKeyConstraint,
    BigInteger,
    DateTime,
    Unicode,
    Numeric,
    select,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, synonym, Mapped

from radar_timeline_data.audit_writer.audit_writer import (
    AuditWriter,
    Heading,
    Change,
    Table,
    StubObject,
    WorkSheet,
    ComparisonTable,
)
from radar_timeline_data.audit_writer.audit_writer import List as Li
from radar_timeline_data.utils.connections import (
    df_batch_insert_to_sql,
    get_data_as_df,
)
from radar_timeline_data.utils.utils import (
    check_nulls_in_column,
    max_with_nulls,
    chunk_list,
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

    audit_writer.add(Heading("Processing Treatments", "Heading 3"))

    df_collection = make_treatment_dfs(
        sessions,
        radar_patient_id_map.filter(pl.col("ukrdcid").is_not_null()).get_column(
            "ukrdcid"
        ),
        radar_patient_id_map.filter(pl.col("rr_no").is_not_null()).get_column("rr_no"),
    )

    df_collection = format_treatment(
        codes,
        df_collection,
        satellite,
        source_group_id_mapping,
        radar_patient_id_map,
        audit_writer,
    )
    audit_writer.set_ws(worksheet_name="treatment_import")
    audit_writer.add(
        Li(
            Heading(
                "Imported Treatment data and applied Formatting to tables:", "Heading 4"
            ),
            [
                Table(
                    text="UKRDC",
                    table=df_collection["ukrdc"],
                    table_name="treatment_ukrdc",
                ),
                Table(
                    text="RADAR",
                    table=df_collection["radar"],
                    table_name="treatment_radar",
                ),
                Table(text="RR", table=df_collection["rr"], table_name="treatment_rr"),
            ],
        )
    )
    audit_writer.add_info(
        "Treatments Imported", ("rr count", str(len(df_collection["rr"])))
    )
    audit_writer.add_info(
        "Treatments Imported", ("radar count", str(len(df_collection["radar"])))
    )
    audit_writer.add_info(
        "Treatments Imported", ("ukrdc count", str(len(df_collection["ukrdc"])))
    )

    audit_writer.set_ws("group_reduce_Treatment")
    audit_writer.add(
        [
            Heading("Grouping and Reducing Treatment data", "Heading 4"),
            """Grouping UKRDC and RR seperatly treatments by modality and patient ID, a treatment can be grouped together if overlapping """
            + """dates exist or dates are within 5 days either side of each other""",
        ]
    )
    df_collection["ukrdc"] = group_and_reduce_ukrdc_or_rr_dataframe(
        df_collection["ukrdc"], audit_writer, "ukrdc"
    )
    df_collection["rr"] = group_and_reduce_ukrdc_or_rr_dataframe(
        df_collection["rr"], audit_writer, "rr"
    )

    combined_dataframe = combine_treatment_dataframes(df_collection)

    audit_writer.set_ws("raw_all_Treatment")
    audit_writer.add(
        [
            Heading("Combining all Data Sources:", "Heading 4"),
            Table(
                text="Combined Treatment data of reduced UKRDC, RR with imported RADAR into one column",
                table=combined_dataframe,
                table_name="raw_all_Treatment",
            ),
            "The data is now consolidated into one table and requires grouping and aggregation.",
        ]
    )

    audit_writer.set_ws("group_reduce_all_Treatment")
    # TODO disscuss wether this is all the grouping required and do it over sourcetypes first
    reduced_dataframe = group_and_reduce_combined_treatment_dataframe(
        combined_dataframe
    )
    audit_writer.add(
        [
            Heading("Grouping and Reducing full Treatment data", "Heading 4"),
            Table(
                text="All treatments have been grouped and reduced",
                table=reduced_dataframe,
                table_name="reduced_combined_Treatment",
            ),
        ]
    )

    existing_treatments, new_treatments = split_combined_dataframe(
        combined_dataframe, reduced_dataframe
    )
    audit_writer.add(
        ComparisonTable(
            table_sheet="Test",
            old_tables=[
                Table(
                    text="old table",
                    table=combined_dataframe,
                    table_name="old",
                )
            ],
            new_table=Table(
                text="new table",
                table=existing_treatments,
                table_name="new",
            ),
            common_keys=["patient_id", "modality"],
        )
    )

    audit_writer.add(
        [
            WorkSheet(name="Treatment_output"),
            Li(
                None,
                [
                    Table(
                        text="data that is new",
                        table=new_treatments,
                        table_name="new_Treatment",
                    ),
                    Table(
                        text="data to update",
                        table=existing_treatments,
                        table_name="update_Treatment",
                    ),
                ],
            ),
        ]
    )
    audit_writer.add_info(
        "treatments output breakdown",
        (
            "total update/create",
            str(len(new_treatments) + len(existing_treatments)),
        ),
    )
    audit_writer.add_info(
        "treatments output breakdown",
        ("to update", str(len(existing_treatments))),
    )
    audit_writer.add_info(
        "treatments output breakdown",
        ("to create", str(len(new_treatments))),
    )

    # =====================< WRITE TO DATABASE >==================
    if commit:
        audit_writer.add("Starting data commit.")
        total_rows, failed_rows = df_batch_insert_to_sql(
            new_treatments, sessions["radar"], radar.Dialysi.__table__, 1000, "id"
        )
        audit_writer.add(f"{total_rows} rows of treatment data added or modified")

        if len(failed_rows) > 0:
            temp = pl.from_dicts(failed_rows)
            audit_writer.add(
                [
                    WorkSheet("errors"),
                    Li(
                        Heading("Treatment data insert failed", "Heading 4"),
                        [
                            Table(
                                text=f"{len(failed_rows)} rows of treatment data failed",
                                table=temp,
                                table_name="failed_treatment_rows",
                            ),
                        ],
                    ),
                ]
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

    ukrdc_pat_map = radar_patient_id_map.drop_nulls(["ukrdcid"]).unique(
        subset=["ukrdcid"]
    )
    rr_pat_map = radar_patient_id_map.drop_nulls(["rr_no"]).unique(subset=["rr_no"])

    audit_writer.add(
        [
            Heading("Formatting Treatments", "Heading 4"),
            [
                [
                    Heading("UKRDC changes", "Heading 5"),
                    [
                        Change(
                            "using the Patient number mapping convert ukrdc patient ids to radar ids",
                            [
                                ["ukrdcid"],
                                ["radar ids"],
                                ["patient_id"],
                            ],
                        ),
                        Change(
                            "replace the source group id with the main unit code",
                            [
                                ["satellite_code"],
                                ["main_unit_code"],
                                ["source_group_id"],
                            ],
                        ),
                        Change(
                            "replace the modality with the equivalent modality code",
                            [
                                ["registry_code"],
                                ["equivalent_modality"],
                                ["modality"],
                            ],
                        ),
                    ],
                ],
                [
                    Heading("RR Changes", "Heading 5"),
                    [
                        Change(
                            "using the Patient number mapping convert ukrdc patient ids to radar ids",
                            [
                                ["rr_no"],
                                ["radar ids"],
                                ["patient_id"],
                            ],
                        ),
                        Change(
                            "replace the source group id with the main unit code",
                            [
                                ["satellite_code"],
                                ["main_unit_code"],
                                ["source_group_id"],
                            ],
                        ),
                        Change(
                            "replace the modality with the equivalent modality code",
                            [
                                ["registry_code"],
                                ["equivalent_modality"],
                                ["modality"],
                            ],
                        ),
                    ],
                ],
            ],
        ]
    )
    # TODO add None default to source_group_id
    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        id=pl.lit(None),
        source_type=pl.lit("UKRDC"),
        source_group_id=pl.col("source_group_id").replace(
            satellite.get_column("satellite_code"),
            satellite.get_column("main_unit_code"),
        ),
    )

    df_collection["rr"] = df_collection["rr"].with_columns(
        id=pl.lit(None),
        source_type=pl.lit("RR"),
        source_group_id=pl.col("source_group_id").replace(
            satellite.get_column("satellite_code"),
            satellite.get_column("main_unit_code"),
        ),
    )

    df_collection["ukrdc"] = df_collection["ukrdc"].with_columns(
        patient_id=pl.col("patient_id").replace(
            ukrdc_pat_map.get_column("ukrdcid"),
            ukrdc_pat_map.get_column("radar_id"),
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

    df_collection["rr"] = df_collection["rr"].with_columns(
        patient_id=pl.col("patient_id").replace(
            rr_pat_map.get_column("rr_no"),
            rr_pat_map.get_column("radar_id"),
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


base = declarative_base()


class Treatment(base):  # type: ignore
    __tablename__ = "TREATMENT"
    __table_args__ = (
        PrimaryKeyConstraint(
            "RR_NO",
            "DATE_START",
            "TREATMENT_MODALITY",
            "HOSP_CENTRE",
            "TREATMENT_CENTRE",
            name="PK_TREATMENT",
        ),
    )

    RR_NO = Column(BigInteger, primary_key=True)
    DATE_START = Column(DateTime, primary_key=True)
    TREATMENT_MODALITY = Column(Unicode(8, "Latin1_General_CI_AS"), primary_key=True)
    TREATMENT_CENTRE = Column(Unicode(8, "Latin1_General_CI_AS"), primary_key=True)
    HOSP_CENTRE = Column(Unicode(8, "Latin1_General_CI_AS"), primary_key=True)
    DATE_END = Column(DateTime)
    ADD_HAEMO_ON_PD = Column(Unicode(1, "Latin1_General_CI_AS"))
    CHANGE_TREATMENT = Column(Unicode(8, "Latin1_General_CI_AS"))
    HAEMO_DIAL_ACCESS = Column(Unicode(8, "Latin1_General_CI_AS"))
    FGS_SITE = Column(Unicode(8, "Latin1_General_CI_AS"))
    HD_CATHETER_SITE = Column(Unicode(8, "Latin1_General_CI_AS"))
    DIALYSER_USED = Column(Unicode(8, "Latin1_General_CI_AS"))
    FLOW_RATE = Column(Numeric(38, 4))
    DIAL_REUSE = Column(Unicode(1, "Latin1_General_CI_AS"))
    TIMES_PER_WEEK = Column(Numeric(20, 0))
    DIAL_TIME = Column(Numeric(38, 4))
    BICARB_DIAL = Column(Unicode(1, "Latin1_General_CI_AS"))
    HD_SUPERVISON = Column(Unicode(4, "Latin1_General_CI_AS"))
    WEEKLY_FLUID_VOL = Column(Numeric(38, 4))
    BAG_SIZE = Column(Numeric(38, 4))
    LOAD_IND = Column(Unicode(1, "Latin1_General_CI_AS"))
    DISPLAY_SEQ = Column(Numeric(38, 4))
    YEAR_END_SEQ = Column(Numeric(38, 4))
    TRANSFER_IN_FROM = Column(Unicode(10, "Latin1_General_CI_AS"))
    TRANSFER_OUT_TO = Column(Unicode(10, "Latin1_General_CI_AS"))

    # Synonyms
    rr_no: Mapped[int] = synonym("RR_NO")
    date_start: Mapped[datetime.datetime] = synonym("DATE_START")
    treatment_modality: Mapped[str] = synonym("TREATMENT_MODALITY")
    treatment_centre: Mapped[str] = synonym("TREATMENT_CENTRE")
    hosp_centre: Mapped[str] = synonym("HOSP_CENTRE")
    date_end: Mapped[Optional[datetime.datetime]] = synonym("DATE_END")
    add_haemo_on_pd: Mapped[Optional[str]] = synonym("ADD_HAEMO_ON_PD")
    change_treatment: Mapped[Optional[str]] = synonym("CHANGE_TREATMENT")
    haemo_dial_access: Mapped[Optional[str]] = synonym("HAEMO_DIAL_ACCESS")
    fgs_site: Mapped[Optional[str]] = synonym("FGS_SITE")
    hd_catheter_site: Mapped[Optional[str]] = synonym("HD_CATHETER_SITE")
    dialyser_used: Mapped[Optional[str]] = synonym("DIALYSER_USED")
    flow_rate: Mapped[Optional[decimal.Decimal]] = synonym("FLOW_RATE")
    dial_reuse: Mapped[Optional[str]] = synonym("DIAL_REUSE")
    times_per_week: Mapped[Optional[decimal.Decimal]] = synonym("TIMES_PER_WEEK")
    dial_time: Mapped[Optional[decimal.Decimal]] = synonym("DIAL_TIME")
    bicarb_dial: Mapped[Optional[str]] = synonym("BICARB_DIAL")
    hd_supervison: Mapped[Optional[str]] = synonym("HD_SUPERVISON")
    weekly_fluid_vol: Mapped[Optional[decimal.Decimal]] = synonym("WEEKLY_FLUID_VOL")
    bag_size: Mapped[Optional[decimal.Decimal]] = synonym("BAG_SIZE")
    load_ind: Mapped[Optional[str]] = synonym("LOAD_IND")
    display_seq: Mapped[Optional[decimal.Decimal]] = synonym("DISPLAY_SEQ")
    year_end_seq: Mapped[Optional[decimal.Decimal]] = synonym("YEAR_END_SEQ")
    transfer_in_from: Mapped[Optional[str]] = synonym("TRANSFER_IN_FROM")
    transfer_out_to: Mapped[Optional[str]] = synonym("TRANSFER_OUT_TO")


def make_treatment_dfs(
    sessions: dict[str, Session], ukrdc_filter: pl.Series, ukrr_filter: pl.Series
) -> dict[str, pl.DataFrame]:
    """
    Convert sessions data into DataFrame collection holding treatments.

    Args:

        ukrr_filter:
        sessions (dict): A dictionary containing session information.
        ukrdc_filter (pl.Series, optional):A filter of ids to pull

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

    str_filter = ukrdc_filter.cast(pl.String).to_list()

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

    df_collection["ukrdc"] = df_collection["ukrdc"].filter(
        pl.col("modality").is_not_null()
    )

    check_nulls_in_column(df_collection["ukrdc"], "modality")

    df_collection["rr"] = pl.DataFrame()
    for chunk in chunk_list(ukrr_filter.cast(pl.String).to_list(), 1000):
        rr_query = select(
            Treatment.rr_no.label("patient_id"),
            Treatment.treatment_centre.label("source_group_id"),
            Treatment.treatment_modality.label("modality"),
            Treatment.date_start.label("from_date"),
            Treatment.date_end.label("to_date"),
        ).filter(Treatment.rr_no.in_(chunk))
        df_chunk = get_data_as_df(sessions["rr"], rr_query)
        df_collection["rr"] = pl.concat([df_collection["rr"], df_chunk])
    df_collection["rr"] = df_collection["rr"].with_columns(
        id=pl.lit(None),
        created_date=pl.lit(None).cast(pl.Date),
        modified_date=pl.lit(None).cast(pl.Date),
    )
    check_nulls_in_column(df_collection["rr"], "from_date")

    df_collection["rr"] = df_collection["rr"].filter(pl.col("modality").is_not_null())

    check_nulls_in_column(df_collection["rr"], "modality")
    return df_collection


def group_and_reduce_ukrdc_or_rr_dataframe(
    df: pl.DataFrame,
    audit_writer: AuditWriter | StubObject,
    name: str,
) -> pl.DataFrame:
    """
    Group and reduce the combined DataFrame by patient_id and group_id.
    The resulting DataFrame contains the first occurrence of each column for each patient-group combination.

    Args:
    - combined_dataframe (DataFrame): The input DataFrame containing combined data.

    Returns:
    - DataFrame: The reduced DataFrame with grouped and aggregated data.
    """

    df = group_similar_or_overlapping_range(df, ["patient_id", "modality"])
    audit_writer.add(
        Table(
            text=f"{name} grouped by patient_id and modality",
            table=df,
            table_name=f"{name}",
        )
    )

    df = df.with_columns(
        pl.max_horizontal(["created_date", "modified_date"]).alias("most_recent_date")
    )

    # for each patient_id, modality, group_id combination where group id represents overlapping dates,
    # we select the earliest from date and latest to date where to date is not null,
    # all other columns are decided by most recent creation or update date regardless of if value is null
    # TODO ask about this VVV
    df = (
        df.sort(
            "most_recent_date",
            descending=True,
        )
        .group_by(["patient_id", "modality", "group_id"])
        .agg(
            pl.col("from_date").min(),
            max_with_nulls(pl.col("to_date")).alias("to_date"),
            **{
                col: pl.col(col).first()
                for col in df.columns
                if col
                not in ["from_date", "to_date", "patient_id", "modality", "group_id"]
            },
        )
    )
    audit_writer.add(
        Table(
            text=f"Reducing {name} to one row per patient_id, modality, and group_id",
            table=df,
            table_name=f"reduced_{name}",
        )
    )

    return df


def group_similar_or_overlapping_range(
    df: pl.DataFrame, window: List[str], day_override: int = 5
) -> pl.DataFrame:
    """
    Group similar or overlapping date ranges within a specified window. ie transplants of similar ranges can be seen as
     a single continous range

    Args:
        df (pl.DataFrame): Input DataFrame containing date ranges.
        window (List[str]): List of column names to partition the data.
        day_override (int): Number of days to consider ranges as overlapping.

    Returns:
        pl.DataFrame: DataFrame with 'group_id' column indicating groupings of similar or overlapping ranges.
    """

    mask = overlapping_dates_bool_mask(days=day_override)
    descending = [False] * len(window) + [False, True]

    # Sorting the data first by 'from_date' in descending order to arrange the entries chronologically, and then by
    # 'to_date' in ascending order to ensure that in the case of date clashes.
    # This method then shifts data so that each row can reference a prior 'from_date'

    df = df.sort(window + ["from_date", "to_date"], descending=descending).with_columns(
        pl.col("from_date").shift().over(window).alias("prev_from_date")
    )

    # Sorting the data by 'to_date' in ascending order to ensure that in the case of date clashes.
    # This method then shifts data so that each row can reference a prior 'to_date'

    df = df.sort(window + ["to_date"], nulls_last=True).with_columns(
        pl.col("to_date").shift().forward_fill().over(window).alias("prev_to_date")
    )
    df = (
        df.with_columns(pl.col("to_date").shift(-1).alias("next_to_date").over(window))
        .with_columns(
            pl.when(pl.col("prev_to_date").is_null())
            .then(pl.col("next_to_date"))
            .otherwise(pl.col("prev_to_date"))
            .over(window)
            .alias("prev_to_date")
        )
        .drop("next_to_date")
    )

    # By sorting the data chronologically, we align the rows so that each entry references the 'from_date' and
    # 'to_date' of the previous row. We then apply a mask to identify where there are gaps or overlaps between the
    # 'from_date' and 'to_date' of consecutive rows. These gaps or overlaps are marked with a 'group_id' of 1,
    # indicating that the current row does not belong to the same group as the previous rows. Finally, we use 'run
    # length encoding' (cumulative sum) on the 'group_id' to assign unique group IDs to consecutive groups of
    # overlapping intervals.

    df = (
        df.sort(window + ["from_date", "to_date"], descending=descending)
        .with_columns(pl.when(mask).then(0).otherwise(1).over(window).alias("group_id"))
        .with_columns(
            pl.col("group_id").cum_sum().rle_id().over(window).alias("group_id")
        )
    )

    return df.drop(["prev_to_date", "prev_from_date"])


# TODO check this is working nulls seem to not overlap
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
            old=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
            new=["0", "1", "2", "3", "4"],
            default=None,
        )
        .cast(pl.Int32)
    )

    combined_dataframe = combined_dataframe.sort(
        ["patient_id", "source_type", "recent_date", "from_date"], descending=True
    )
    # TODO check this as it was 15 and patient id
    combined_dataframe = group_similar_or_overlapping_range(
        combined_dataframe, ["patient_id", "modality"], 5
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
    # TODO chcek with other source TODO
    return (
        reduced_dataframe.sort(
            ["patient_id", "source_type", "recent_date", "from_date"],
            descending=True,
        )
        .group_by(["patient_id", "modality", "group_id"])
        .agg(
            pl.col("id").filter(pl.col("id").is_not_null()).first(),
            **{
                col: pl.col(col).first()
                for col in reduced_dataframe.columns
                if col not in ["id", "patient_id", "modality", "group_id"]
            },
        )
        .drop("group_id")
        .with_columns(
            source_type=pl.col("source_type")
            .cast(pl.String)
            .replace(
                new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
                old=["0", "1", "2", "3", "4"],
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
    full_dataframe = full_dataframe.select(reduced_dataframe.columns)

    new_rows = reduced_dataframe.filter(pl.col("id").is_null())

    # update treatments should have created_date dropped to not overwrite and should have modified set to current
    existing_rows = reduced_dataframe.filter(pl.col("id").is_not_null())

    temp = existing_rows.join(
        full_dataframe.with_columns(
            source_type=pl.col("source_type")
            .cast(pl.String)
            .replace(
                new=["NHSBT LIST", "BATCH", "UKRDC", "RADAR", "RR"],
                old=["0", "1", "2", "3", "4"],
                default=None,
            )
        ),
        on="id",
        how="left",
        suffix="_old",
    )
    cols = [col for col in existing_rows.columns if col not in ["id"]]
    existing_rows = temp.filter(mask(cols))
    existing_rows = (
        existing_rows.select(cols + ["created_date_old"] + ["id"])
        .with_columns(pl.col("created_date_old").alias("created_date"))
        .drop("created_date_old")
    )
    # TODO Double check this
    return existing_rows, new_rows


def mask(cols):
    conditions = [pl.col(col) != pl.col(f"{col}_old") for col in cols]
    combined_condition = reduce(or_, conditions)
    return combined_condition
