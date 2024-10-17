from datetime import datetime, timedelta

import pytest
import polars as pl
from faker import Faker

from radar_timeline_data.audit_writer import StubObject
from radar_timeline_data.utils.transplants import group_and_reduce_transplant_rr

fake = Faker()
start_date = datetime.strptime("2016-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-01-01", "%Y-%m-%d")


def single_patient_dataframe(total, patient_id, modality, date_overide):
    date = fake.date_between(start_date, end_date)
    from_date = pl.Series(
        "from_date",
        [date + timedelta(days=date_overide * i) for i in range(total)],
    )
    date_of_failure = pl.Series(
        "to_date",
        [i + timedelta(days=10) for i in from_date],
    )

    return (
        [patient_id for _ in range(total)],
        [modality for _ in range(total)],
        from_date,
        date_of_failure,
        date_overide,
    )


@pytest.mark.parametrize(
    "patient_id, modality, date, date_of_failure,date_overide",
    [single_patient_dataframe(100, 1, 1, 5), single_patient_dataframe(100, 1, 1, 10)],
)
def test_group_reduce(patient_id, modality, date, date_of_failure, date_overide):
    df = pl.DataFrame(
        {
            "patient_id": patient_id,
            "modality": modality,
            "date": date,
            "date_of_failure": date_of_failure,
        }
    )
    df = {"rr": df}

    result = group_and_reduce_transplant_rr(StubObject(), df)["rr"]
    assert result.shape[0] == (100 if date_overide == 10 else 1)
    assert result["date"].min() == date.min()


@pytest.mark.parametrize("total", [10, 50, 100, 1000])
def test_multiple_modality_and_one_patient(total):
    data = None
    for i in range(total):
        (
            patient_id,
            modality,
            date,
            date_of_failure,
            date_overide,
        ) = single_patient_dataframe(10, 1, i, 5)
        df = pl.DataFrame(
            {
                "patient_id": patient_id,
                "modality": modality,
                "date": date,
                "date_of_failure": date_of_failure,
            }
        )
        data = pl.concat([data, df]) if data is not None else df

    data = group_and_reduce_transplant_rr(StubObject(), {"rr": data})["rr"]

    assert data.shape[0] == total
    assert data.select(pl.col("modality").n_unique()).item() == total


@pytest.mark.parametrize("total", [10, 50, 100, 1000])
def test_single_modality_multiple_patients(total):
    data = None
    for i in range(total):
        (
            patient_id,
            modality,
            date,
            date_of_failure,
            date_overide,
        ) = single_patient_dataframe(10, i, 10, 5)
        df = pl.DataFrame(
            {
                "patient_id": patient_id,
                "modality": modality,
                "date": date,
                "date_of_failure": date_of_failure,
            }
        )
        data = pl.concat([data, df]) if data is not None else df

    data = group_and_reduce_transplant_rr(StubObject(), {"rr": data})["rr"]

    assert data.shape[0] == total
    assert data.select(pl.col("patient_id").n_unique()).item() == total
