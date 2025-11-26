# this file will have certain aspects that will act as a config for diffrent methods
import polars as pl
import radar_models.radar2 as radar
from ukrdc_sqla.ukrdc import column_names as col
from ukrr_models.nhsbt_models import UKTTransplant, UKTSites

# a dict that handles specific overrides, they will be converted to capital keys in certain locations
override_dict = {
    "externalid": pl.String,
    "donor_hla": pl.String,
    "recipient_hla": pl.String,
    "graft_loss_cause": pl.String,
    "date_of_cmv_infection": pl.Date,
    "date": pl.Date,
    "date_of_failure": pl.Date,
    "date_of_recurrence": pl.Date,
    "chi_no": pl.String,
    "hsc_no": pl.String,
    "new_nhs_no": pl.String,
    "radar_id": pl.String,
    "rr_no": pl.String,
}

# this is allows for the mapping from one db type to another using column strings
rr_to_radar_columns = {
    col(UKTTransplant.HLA_MISMATCH): col(radar.Transplant.mismatch_hla),
    # col(UKTTransplant.rr_no): col(radar.Transplant.patient_id),
    col(UKTTransplant.transplant_type): col(radar.Transplant.modality),
    col(UKTTransplant.transplant_date): col(radar.Transplant.date),
    col(UKTTransplant.ukt_fail_date): col(radar.Transplant.date_of_failure),
    col(UKTTransplant.hla_mismatch): col(radar.Transplant.mismatch_hla),
    col(UKTSites.rr_code): col(radar.Transplant.source_group_id),
}
