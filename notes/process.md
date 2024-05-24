# Treatment Data Processing Guide

## Data Import

1. **Import Treatment Data**
    - Sources: RADAR, UKRDC

2. **Import Source ID and Codes**
    - Source: RADAR

## Data Formatting Steps for UKRDC Data

### Patient ID Mapping

- Replace `pid` with `patient_id` using the UKRDC mapping. If no mapping exists, set the value to `None`.

### Healthcare Facility Code Conversion

- Convert satellite facility codes to their corresponding main unit codes.

### Source Group ID Conversion

- Convert `healthcarefacility_code` into `source_group_id` using the source group mapping.

### Admit Reason Code Conversion

- Convert the admit reason code to RR7 codes.

### Field Name Changes

Rename fields as follows:

- `creation_date` -> `created_date`
- `update_date` -> `modified_date`
- `fromtime` -> `from_date`
- `totime` -> `to_date`
- `admitreasoncode` -> `modality`

### Type Changes

- Ensure all data types match the required schema.

### Data Cleanup

- Clean up the following:
    - Codes
    - UKRDC_Radar_Mapping
    - Satellite facility codes
    - Columns

## UKRDC Data Reduction and Deduplication Process

1. **Check for Null Values in `from_date`**
    - Raise an error if any `from_date` values are null.

2. **Sort Data**
    - Sort by `patient_id` (descending), `modality`, `from_date` (descending), and `to_date` (ascending).

3. **Shift `from_date`**
    - Shift `from_date` within each `patient_id` and `modality` to obtain the previous date.

4. **Sort Data Again**
    - Sort by `patient_id`, `modality`, and `to_date` (treating null values as the greatest value).

5. **Shift and Forward Fill Nulls**
    - Shift the data and forward fill any null values to obtain the previous `from_date`.

6. **Sort Data Again**
    - Sort by `patient_id`, `modality`, `from_date`, and `to_date`.

7. **Check for Overlapping Dates**
    - Compare the previous `to_date` and `from_date` with the current row's dates to determine overlaps.

8. **Apply Run-Length Encoding**
    - Use run-length encoding to group overlapping dates by 5 days.
    - Remove shifted dates.

9. **Get Recent Date**
    - Use the most recent date from the max of `created_date` or `modified_date`.

10. **Sort by Recent Date**
    - Form groups by `patient_id` and `modality`, then apply run-length encoding.

11. **Regroup Overlapping Dates**
    - Regroup by `patient_id` for modalities overlapping by 15 days and apply the same logic to reduce data.

12. **Combine Treatment Data**
    - Group ranges by 15 days.

13. **Validate Data**
    - Sort by data validity and select the first non-null ID. If not available, use the null ID and select the first values for other columns.

## EXPORT Data


---
# Transplant Data

## Step 1: Import Data

- Import transplant data from UKRR and RADAR.

## Step 2: Map and Drop `RR_NO`

- Replace the `RR_NO` column with `patient_id` using the `rr_radar_mapping` and then drop the `RR_NO` column.

## Step 3: Transplant Unit Conversion

- Apply the `convert_transplant_unit` function to `df_collection` with the `sessions` data.

## Step 4: Update Transplant Modality

- Apply the `get_rr_transplant_modality` function to `df_collection["rr"]`.

## Step 5: Rename Columns

- Rename the following columns:
    - `TRANSPLANT_UNIT` to `transplant_group_id`
    - `UKT_FAIL_DATE` to `date_of_failure`
    - `TRANSPLANT_DATE` to `date`
    - `HLA_MISMATCH` to `hla_mismatch`

## Step 6: Drop Unnecessary Columns

- Drop the following columns:
    - `TRANSPLANT_TYPE`
    - `TRANSPLANT_ORGAN`
    - `TRANSPLANT_RELATIONSHIP`
    - `TRANSPLANT_SEX`

## Step 7: Add New Columns

- Add the following columns with static values:
    - `source_group_id` with the value `200`
    - `source_type` with the value `"RR"`

## Group and Reduce

### UKRR Transplants

1. Sort the UKRR transplants by `patient_id` and `date`.
2. Shift dates within the same `patient_id`.
3. Apply run-length encoding (RLE) on overlapping dates.
4. Select the first values of each group.

### Combine RADAR and UKRR

1. Combine the RADAR and UKRR transplants.
2. Sort the combined transplants by `patient_id` and `date`.
3. Shift dates within the same `patient_id`.
4. Apply run-length encoding (RLE) on overlapping dates.
5. Select the first values of each group and the first non-null ID if available.