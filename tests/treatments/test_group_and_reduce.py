from datetime import datetime, timedelta

import pytest
import polars as pl
from faker import Faker

from radar_timeline_data.audit_writer.audit_writer import StubObject
from radar_timeline_data.utils.treatments import group_and_reduce_ukrdc_or_rr_dataframe

fake = Faker()
start_date = datetime.strptime("2016-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-01-01", "%Y-%m-%d")


def single_patient_dataframe(total, patient_id, modality):
    date = fake.date_between(start_date, end_date)
    from_date = pl.Series(
        "from_date",
        [date + timedelta(days=5 * i) for i in range(total)],
    )
    to_date = pl.Series(
        "to_date",
        [i + timedelta(days=10) for i in from_date],
    )

    created_date = pl.Series(
        "created_date",
        [i for i in from_date],
    )
    modified_date = pl.Series(
        "modified_date",
        [i for i in from_date],
    )

    return (
        [patient_id for _ in range(total)],
        [modality for _ in range(total)],
        from_date,
        to_date,
        created_date,
        modified_date,
    )


@pytest.mark.parametrize(
    "patient_id, modality, from_date, to_date, created_date, modified_date",
    [single_patient_dataframe(100, 1, 1)],
)
def test_single_modality_and_patient(
    patient_id, modality, from_date, to_date, created_date, modified_date
):
    """Testing outcome of dataframe with only 1 distinct patient and modality"""

    df = pl.DataFrame(
        {
            "patient_id": patient_id,
            "modality": modality,
            "from_date": from_date,
            "to_date": to_date,
            "created_date": created_date,
            "modified_date": modified_date,
        }
    )
    df = group_and_reduce_ukrdc_or_rr_dataframe(df, StubObject(), "none")
    # Step 3: Assert there is only 1 row in the result
    assert df.shape[0] == 1, f"Expected 1 row, got {df.shape[0]}"

    # Step 4: Check that from_date equals the minimum of the input `from_date`

    assert df["from_date"][0] == from_date.min()

    # Step 5: Check that to_date equals the maximum of the input `to_date`
    assert (
        df["to_date"][0] == to_date.max()
    ), f"Expected to_date to be {max(to_date)}, got {df['to_date'][0]}"


def test_multiple_modality_and_one_patient():
    data = None
    for i in range(10):
        (
            patient_id,
            modality,
            from_date,
            to_date,
            created_date,
            modified_date,
        ) = single_patient_dataframe(10, 1, i)
        df = pl.DataFrame(
            {
                "patient_id": patient_id,
                "modality": modality,
                "from_date": from_date,
                "to_date": to_date,
                "created_date": created_date,
                "modified_date": modified_date,
            }
        )
        data = pl.concat([data, df]) if data is not None else df

    data = group_and_reduce_ukrdc_or_rr_dataframe(data, StubObject(), "none")
    assert data.shape[0] == 10
    assert data.select(pl.col("modality").n_unique()).item() == 10


def test_single_modality_multiple_patients():
    data = None
    for i in range(10):
        (
            patient_id,
            modality,
            from_date,
            to_date,
            created_date,
            modified_date,
        ) = single_patient_dataframe(10, i, 10)
        df = pl.DataFrame(
            {
                "patient_id": patient_id,
                "modality": modality,
                "from_date": from_date,
                "to_date": to_date,
                "created_date": created_date,
                "modified_date": modified_date,
            }
        )
        data = pl.concat([data, df]) if data is not None else df

    data = group_and_reduce_ukrdc_or_rr_dataframe(data, StubObject(), "none")
    assert data.shape[0] == 10
    assert data.select(pl.col("patient_id").n_unique()).item() == 10


@pytest.mark.parametrize("total", [(10), (100), (1000)])
def test_multiple_modality_multiple_patients(total):
    data = None
    for i in range(10):
        for j in range(10):
            (
                patient_id,
                modality,
                from_date,
                to_date,
                created_date,
                modified_date,
            ) = single_patient_dataframe(total, i, j)
            df = pl.DataFrame(
                {
                    "patient_id": patient_id,
                    "modality": modality,
                    "from_date": from_date,
                    "to_date": to_date,
                    "created_date": created_date,
                    "modified_date": modified_date,
                }
            )
            data = pl.concat([data, df]) if data is not None else df

    data = group_and_reduce_ukrdc_or_rr_dataframe(data, StubObject(), "none")
    assert data.shape[0] == 100
    assert data.select(pl.col("patient_id").n_unique()).item() == 10
    # Gcheck that each patient has 10 uniquie modalitys
    assert (
        data.groupby("patient_id")
        .agg(pl.col("modality").n_unique())
        .select(pl.col("modality").alias("n_modalities"))
        .filter(pl.col("n_modalities") != 10)
        .is_empty()
    )

    # Check that the date differences are expected
    assert (
        data.with_columns(
            (pl.col("to_date") - pl.col("from_date")).dt.days().alias("date_diff")
        )
        .filter(pl.col("date_diff") != ((5 * total) + 5))
        .is_empty()
    ), "Not all date differences are 5"


def test_date_priority():
    """Testing cases in which values can be grouped but contains inconsistency in certain fields and therefore
    requires priority of most recently created/modified"""
    date = fake.date_between(start_date, end_date)
    created_date = [date for _ in range(9)] + [date + timedelta(10)]
    modified_date = [date for _ in range(10)]
    df = pl.DataFrame(
        {
            "patient_id": [1 for _ in range(10)],
            "modality": [1 for _ in range(10)],
            "from_date": date,
            "to_date": date + timedelta(1),
            "created_date": created_date,
            "modified_date": modified_date,
            "test": [i for i in range(10)],
        }
    )

    df = group_and_reduce_ukrdc_or_rr_dataframe(df, StubObject(), "obj")
    assert df["test"][0] == 9
