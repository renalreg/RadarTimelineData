from datetime import datetime, timedelta

import pytest
import polars as pl
from faker import Faker
from polars import ColumnNotFoundError, ComputeError

from radar_timeline_data.utils.treatments import (
    combine_treatment_dataframes,
    group_and_reduce_combined_treatment_dataframe,
)

fake = Faker()
start_date = datetime.strptime("2016-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-01-01", "%Y-%m-%d")


def test_combine_one_patient_multiple_sources():
    date = fake.date_between(start_date, end_date)
    dataframes = {}
    for source in ["UKRDC", "RR", "RADAR", "BATCH", "NHSBT LIST"]:
        df = pl.DataFrame(
            {
                "patient_id": [1 for _ in range(10)],
                "modality": [1 for _ in range(10)],
                "from_date": [date + timedelta(5 * i) for i in range(10)],
                "to_date": [date + timedelta((5 * i) + 1) for i in range(10)],
                "created_date": start_date,
                "modified_date": start_date,
                "source_type": [source for i in range(10)],
            }
        )
        dataframes[source] = df
    df = combine_treatment_dataframes(dataframes)
    assert df.shape[0] == 50


@pytest.mark.parametrize(
    "total",
    [10, 100, 1000],
)
def test_multiple_source_dataframe_reduction(total):
    """Testing the interactions between multiple data sources and the regards to priority"""
    date = fake.date_between(start_date, end_date)
    dataframes = {}
    for source in [
        "RADAR",
        "UKRDC",
        "RR",
    ]:
        df = pl.DataFrame(
            {
                "patient_id": [1 for _ in range(total)],
                "modality": [1 for _ in range(total)],
                "from_date": [date + timedelta(10 * i) for i in range(total)],
                "to_date": [date + timedelta((10 * i) + 1) for i in range(total)],
                "created_date": start_date,
                "modified_date": start_date,
                "source_group_id": [1 for _ in range(total)],
                "source_type": [source for i in range(total)],
            }
        )
        if source == "RADAR":
            df = df.with_columns(pl.Series("id", [fake.uuid4() for _ in range(total)]))
        dataframes[source] = df
    df = combine_treatment_dataframes(dataframes)

    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)

    # as each had equal values in each group the id should be present
    assert df["id"].is_null().any() == False

    dataframes["RADAR"] = dataframes["RADAR"].head(total // 2)
    df = combine_treatment_dataframes(dataframes)

    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)
    assert df["id"].is_null().any() == True

    # checking that radar has been replaced with the rr data
    assert df.filter(pl.col("source_type") == "RADAR").is_empty()

    dataframes.pop("RR")
    df = combine_treatment_dataframes(dataframes)
    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)
    # assert that sourcetype of ukrdc is in final reduction as there is no rr or radar rows that match
    assert not df.filter(pl.col("source_type") == "UKRDC").is_empty()


def test_expected_reduction():
    """Testing with manual defiend dataframes that the correct output occurs"""
    date = datetime.now().date()
    dataframes = {
        "RADAR": pl.DataFrame(
            {
                "id": [fake.uuid4() for _ in range(4)],
                "patient_id": [1, 2, 3, 4],
                "modality": [1, 2, 3, 4],
                "from_date": [date for _ in range(4)],
                "to_date": [date + timedelta(10) for _ in range(4)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1 for _ in range(4)],
                "source_type": ["RADAR" for i in range(4)],
            }
        ),
        "RR": pl.DataFrame(
            {
                "patient_id": [1, 5],
                "modality": [10, 2],
                "from_date": [date + timedelta(12) for _ in range(2)],
                "to_date": [date + timedelta(15) for _ in range(2)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1 for _ in range(2)],
                "source_type": ["RR" for i in range(2)],
            }
        ),
        "UKRDC": pl.DataFrame(
            {
                "patient_id": [5, 6],
                "modality": [1, 2],
                "from_date": [date + timedelta(14) for _ in range(2)],
                "to_date": [date + timedelta(19) for _ in range(2)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1 for _ in range(2)],
                "source_type": ["UKRDC" for i in range(2)],
            }
        ),
    }
    df = combine_treatment_dataframes(dataframes)

    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)

    # check that the rows sources have correctly been updated
    assert df.shape[0] == 8
    assert df.filter(pl.col("source_type") == "RADAR").shape[0] == 4
    assert df.filter(pl.col("source_type") == "UKRDC").shape[0] == 2
    assert df.filter(pl.col("source_type") == "RR").shape[0] == 2


def test_overlapping_dates():
    """Test behavior when date ranges overlap between sources."""
    date = datetime.now().date()
    dataframes = {
        "RADAR": pl.DataFrame(
            {
                "id": [fake.uuid4() for _ in range(2)],
                "patient_id": [1, 2],
                "modality": [1, 2],
                "from_date": [date, date],
                "to_date": [date + timedelta(10), date + timedelta(5)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1, 1],
                "source_type": ["RADAR" for _ in range(2)],
            }
        ),
        "RR": pl.DataFrame(
            {
                "patient_id": [1, 2],
                "modality": [10, 2],
                "from_date": [date + timedelta(3), date + timedelta(4)],
                "to_date": [date + timedelta(12), date + timedelta(8)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1, 1],
                "source_type": ["RR" for _ in range(2)],
            }
        ),
    }
    df = combine_treatment_dataframes(dataframes)

    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)
    assert df.shape[0] == 3  # Overlapping dates should reduce rows
    assert (
        df.filter(pl.col("source_type") == "RR").shape[0] == 2
    )  # Priority should be RR over RADAR


def test_non_overlapping_dates():
    """Test behavior when date ranges are non-overlapping between sources."""
    date = datetime.now().date()
    dataframes = {
        "RADAR": pl.DataFrame(
            {
                "id": [fake.uuid4()],
                "patient_id": [1],
                "modality": [1],
                "from_date": [date],
                "to_date": [date + timedelta(10)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1],
                "source_type": ["RADAR"],
            }
        ),
        "RR": pl.DataFrame(
            {
                "patient_id": [1],
                "modality": [2],
                "from_date": [date + timedelta(30)],  # Non-overlapping
                "to_date": [date + timedelta(40)],
                "created_date": date,
                "modified_date": date,
                "source_group_id": [1],
                "source_type": ["RR"],
            }
        ),
    }
    df = combine_treatment_dataframes(dataframes)

    df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)

    assert df.shape[0] == 2  # Should have two distinct non-overlapping periods
    assert df.filter(pl.col("source_type") == "RADAR").shape[0] == 1
    assert df.filter(pl.col("source_type") == "RR").shape[0] == 1


def test_missing_columns():
    """Test how the function handles missing important columns."""
    date = datetime.now().date()
    dataframes = {
        "RADAR": pl.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "modality": [1, 2, 3],
                "from_date": [date for _ in range(3)],
                # "to_date" is missing
                "created_date": [date for _ in range(3)],
                "modified_date": [date for _ in range(3)],
                "source_group_id": [1 for _ in range(3)],
                "source_type": ["RADAR" for _ in range(3)],
            }
        ),
    }
    try:
        df = combine_treatment_dataframes(dataframes)
        df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)
        assert False, "This should raise an error due to missing columns."
    except Exception as e:
        print(e)
        assert isinstance(
            e, ColumnNotFoundError
        ), "Expected KeyError for missing 'to_date' column."


def test_invalid_data_types():
    """Test how the function handles invalid data types in columns."""
    date = datetime.now().date()
    dataframes = {
        "RADAR": pl.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "modality": [1, 2, 3],
                "from_date": [
                    "invalid_date1",
                    "invalid_date2",
                    "invalid_date3",
                ],  # Invalid string dates
                "to_date": [date + timedelta(10) for _ in range(3)],
                "created_date": [date for _ in range(3)],
                "modified_date": [date for _ in range(3)],
                "source_group_id": [1 for _ in range(3)],
                "source_type": ["RADAR" for _ in range(3)],
            }
        ),
    }
    try:
        df = combine_treatment_dataframes(dataframes)
        df: pl.DataFrame = group_and_reduce_combined_treatment_dataframe(df)
        assert False, "This should raise an error due to invalid date types."
    except Exception as e:
        assert isinstance(
            e, ComputeError
        ), "Expected ValueError for invalid date types."
