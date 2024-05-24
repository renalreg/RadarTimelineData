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



# Transplant Data

