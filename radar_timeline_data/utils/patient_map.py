import polars as pl
import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.rr_models as rr
from sqlalchemy import case, select

from radar_timeline_data.utils.connections import get_data_as_df
from radar_timeline_data.utils import chunk_list

radar_pat_query = (
    select(
        radar.PatientNumber.patient_id.label("radar_id"),
        radar.PatientDemographic.date_of_birth,
        case(
            (
                radar.PatientNumber.number_group_id == 120
                and radar.PatientNumber.source_type == "RADAR",
                radar.PatientNumber.number,
            ),
        ).label("nhs_no"),
        case(
            (
                radar.PatientNumber.number_group_id == 121
                and radar.PatientNumber.source_type == "RADAR",
                radar.PatientNumber.number,
            ),
        ).label("chi_no"),
        case(
            (
                radar.PatientNumber.number_group_id == 122
                and radar.PatientNumber.source_type == "RADAR",
                radar.PatientNumber.number,
            ),
        ).label("hsc_no"),
    )
    .join(
        radar.PatientDemographic,
        radar.PatientNumber.patient_id == radar.PatientDemographic.patient_id,
    )
    .filter(radar.PatientNumber.source_type == "RADAR")
    .filter(radar.PatientDemographic.source_type == "RADAR")
    .filter(radar.PatientNumber.number_group_id.in_([120, 121, 122]))
    .order_by(radar.PatientNumber.patient_id)
)


ukrdc_pat_query = (
    select(ukrdc.PatientRecord.ukrdcid, ukrdc.PatientNumber.patientid.label("radar_id"))
    .join(ukrdc.PatientNumber, ukrdc.PatientRecord.pid == ukrdc.PatientNumber.pid)
    .filter(ukrdc.PatientNumber.organization == "RADAR")
    .order_by(ukrdc.PatientNumber.patientid)
)


def map_rr_to_indentifier(connection, identifier_list, identifier_type):
    """
    Map RR numbers to identifiers.

    Args:
        connection: Database connection.
        identifier_list (list): List of identifiers.
        identifier_type (str): Type of identifier.

    Returns:
        DataFrame: DataFrame with RR numbers mapped to identifiers.
    """

    rr_pats = pl.DataFrame()
    for chunk in chunk_list(identifier_list, 1000):
        rr_nhs_query = select(
            rr.UKRRPatient.rr_no,
            identifier_type,
        ).filter(
            identifier_type.in_(chunk),
        )

        df_chunk = get_data_as_df(connection, rr_nhs_query)
        rr_pats = pl.concat([rr_pats, df_chunk])
    return rr_pats


def add_rr_no_to_map(pat_map: pl.DataFrame, rr_pats: pl.DataFrame, identifier):
    """
    Add RR number to patient map.

    Args:
        pat_map (DataFrame): The patient map DataFrame.
        rr_pats (DataFrame): DataFrame containing RR patients data.
        identifier (str): Identifier for joining the DataFrames.

    Returns:
        DataFrame: Updated patient map with RR number added.
    """
    rr_pats = rr_pats.rename({col: col.lower() for col in rr_pats.columns})
    rr_pats = rr_pats.with_columns(pl.col(identifier).cast(pl.String).alias(identifier))
    pat_map = pat_map.join(
        rr_pats.select([identifier, "rr_no"]), on=identifier, how="left"
    )

    if "rr_no_right" in pat_map.columns:
        pat_map = pat_map.with_columns(
            pl.coalesce(["rr_no", "rr_no_right"]).alias("combined_rr")
        )
        pat_map = pat_map.drop(["rr_no", "rr_no_right"])
        pat_map = pat_map.rename({"combined_rr": "rr_no"})

    return pat_map


def make_patient_map(connections) -> pl.DataFrame:
    radar_pats: pl.DataFrame = get_data_as_df(connections["radar"], radar_pat_query)
    ukrdc_pats: pl.DataFrame = get_data_as_df(connections["ukrdc"], ukrdc_pat_query)
    pat_map = radar_pats.join(
        ukrdc_pats, left_on="radar_id", right_on="radar_id", how="left"
    )

    nhs_list = pat_map["nhs_no"].drop_nulls().to_list()
    chi_list = pat_map["chi_no"].drop_nulls().to_list()
    hsc_list = pat_map["hsc_no"].drop_nulls().to_list()

    rr_nhs_map = map_rr_to_indentifier(
        connections["rr"], nhs_list, rr.UKRRPatient.nhs_no
    )
    rr_nhs_map = rr_nhs_map.rename({"NEW_NHS_NO": "NHS_NO"})

    rr_chi_map = map_rr_to_indentifier(
        connections["rr"], chi_list, rr.UKRRPatient.chi_no
    )

    rr_hsc_map = map_rr_to_indentifier(
        connections["rr"], hsc_list, rr.UKRRPatient.hsc_no
    )

    pat_map = add_rr_no_to_map(pat_map, rr_nhs_map, "nhs_no")
    pat_map = add_rr_no_to_map(pat_map, rr_chi_map, "chi_no")
    pat_map = add_rr_no_to_map(pat_map, rr_hsc_map, "hsc_no")
    pat_map = pat_map.unique()

    return pat_map
