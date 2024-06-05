import radar_models.radar2 as radar
import ukrdc_sqla.ukrdc as ukrdc
import ukrr_models.rr_models as rr
from sqlalchemy import case, select
import polars as pl

from radar_timeline_data.utils.connections import get_data_as_df


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


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def map_rr_to_indentifier(connection, identifier_list, identifier_type):
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


def add_rr_no_to_map(pat_map, rr_pats, identifier):
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
    rr_nhs_map = rr_nhs_map.rename({"new_nhs_no": "nhs_no"})

    rr_chi_map = map_rr_to_indentifier(
        connections["rr"], chi_list, rr.UKRRPatient.chi_no
    )

    rr_hsc_map = map_rr_to_indentifier(
        connections["rr"], hsc_list, rr.UKRRPatient.hsc_no
    )
    pat_map: pl.DataFrame
    pat_map = add_rr_no_to_map(pat_map, rr_nhs_map, "nhs_no")
    pat_map = add_rr_no_to_map(pat_map, rr_chi_map, "chi_no")
    pat_map = add_rr_no_to_map(pat_map, rr_hsc_map, "hsc_no")
    pat_map = pat_map.unique()

    return pat_map
