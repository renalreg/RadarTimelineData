from datetime import timedelta

import polars as pl
import pytest
from faker import Faker

from radar_timeline_data.utils.treatments import group_similar_or_overlapping_range

fake = Faker()


@pytest.fixture
def sample_data():
    return pl.DataFrame(
        {
            "from_date": [
                "2023-01-01",
                "2023-01-05",
                "2023-01-10",
                "2023-02-01",
                "2023-01-01",
                "2023-01-06",
                "2023-01-20",
                "2023-02-02",
            ],
            "to_date": [
                "2023-01-03",
                "2023-01-07",
                "2023-01-15",
                "2023-02-05",
                "2023-01-04",
                "2023-01-08",
                "2023-01-21",
                "2023-02-06",
            ],
            "category": ["A", "A", "A", "A", "B", "B", "B", "B"],
        }
    ).with_columns([pl.col("from_date").cast(pl.Date), pl.col("to_date").cast(pl.Date)])


def test_empty_dataframe():
    """Check that the function returns an empty dataframe with the new group_id column"""
    df = pl.DataFrame({"from_date": [], "to_date": [], "category": []})
    result = group_similar_or_overlapping_range(df, ["category"])
    assert result.frame_equal(
        pl.DataFrame({"from_date": [], "to_date": [], "category": [], "group_id": []})
    )


def test_single_row_dataframe():
    """Check that the function returns a group id"""
    df = pl.DataFrame(
        {"from_date": ["2023-01-01"], "to_date": ["2023-01-03"], "category": ["A"]}
    ).with_columns([pl.col("from_date").cast(pl.Date), pl.col("to_date").cast(pl.Date)])

    result = group_similar_or_overlapping_range(df, window=["category"])
    expected = df.with_columns(pl.lit(0).alias("group_id"))
    assert result.frame_equal(expected)


def test_non_overlapping_ranges(sample_data):
    """check that non overlapping ranges are correctly diffrent groups"""
    df = sample_data.filter(pl.col("category") == "A")
    result = group_similar_or_overlapping_range(df, window=["category"], day_override=1)

    # Here, no overlap happens as we set the day_override to 1 (too small to cover gaps)
    expected = df.with_columns(pl.Series("group_id", [0, 1, 2, 3]))
    assert result.frame_equal(expected)


def test_overlapping_ranges_with_override(sample_data):
    """test that overlapping ranges are correctly grouped"""
    df = sample_data.filter(pl.col("category") == "A")
    result = group_similar_or_overlapping_range(
        df, window=["category"], day_override=100
    )

    # In this case, the ranges should be grouped since the day_override allows overlaps within 5 days
    expected = df.with_columns(pl.Series("group_id", [0, 0, 0, 0]))
    assert result.frame_equal(expected)


def test_multiple_partitions(sample_data):
    """test that the window(category) with multiple distinct values are correctly grouped"""
    result = group_similar_or_overlapping_range(
        sample_data, window=["category"], day_override=5
    )

    # Expected group IDs, one group per partition 'A' and 'B'
    expected = sample_data.with_columns(
        [pl.Series("group_id", [0, 0, 0, 1, 0, 0, 1, 2])]
    )
    assert result.frame_equal(expected)


def test_with_small_day_override(sample_data):
    """test that the window(category) with multiple distinct values are correctly grouped when the day_override is
    too small to cover gaps"""
    result = group_similar_or_overlapping_range(
        sample_data, window=["category"], day_override=1
    )

    # With day_override=1, we expect smaller groups since fewer overlaps are possible
    expected = sample_data.with_columns(
        [pl.Series("group_id", [0, 1, 2, 3, 0, 1, 2, 3])]
    )
    assert result.frame_equal(expected)


def test_multiple_categories_no_overlap(sample_data):
    """Testing that multiple distinct values in categories are treated as seperate"""
    data = sample_data.with_columns([pl.Series("patient_id", [1, 1, 2, 2, 1, 1, 1, 1])])
    result = group_similar_or_overlapping_range(data, window=["category", "patient_id"])

    # Expected group IDs, one group per partition 'A' and 'B'
    expected = data.with_columns([pl.Series("group_id", [0, 0, 0, 1, 0, 0, 1, 2])])
    assert result.frame_equal(expected)
    expected = data.sort(["patient_id", "category"]).with_columns(
        [pl.Series("group_id", [0, 0, 0, 0, 1, 2, 0, 1])]
    )
    result = group_similar_or_overlapping_range(data, window=["patient_id", "category"])
    assert result.frame_equal(expected)


@pytest.mark.parametrize("from_date", [(fake.date_this_month()) for _ in range(5)])
def test_null_to_dates(from_date):
    """Test that null to_dates are correctly handled and rows are correctly grouped"""
    dates = pl.Series(
        "from_date", [from_date + timedelta(days=10 * i) for i in range(10)]
    )
    df = (
        pl.DataFrame({"from_date": dates})
        .with_columns(pl.lit(None).alias("to_date"), pl.lit("A").alias("category"))
        .with_columns(
            [pl.col("from_date").cast(pl.Date), pl.col("to_date").cast(pl.Date)]
        )
    )

    result = group_similar_or_overlapping_range(df, window=["category"])
    assert result.frame_equal(
        df.with_columns(pl.Series("group_id", [i for i in range(10)]))
    )

    result = group_similar_or_overlapping_range(
        df, window=["category"], day_override=30
    )
    assert result.frame_equal(
        df.with_columns(pl.Series("group_id", [0 for i in range(10)]))
    )


@pytest.mark.parametrize("base_date", [fake.date_this_month() for _ in range(5)])
def test_null_from_dates(base_date):
    """testing that null from_dates are correctly handled and rows are correctly grouped"""

    # Generate a series of dates based on the base date
    dates = pl.Series("to_date", [base_date + timedelta(days=5 * i) for i in range(10)])

    # Create a DataFrame with the necessary columns
    df = pl.DataFrame({"to_date": dates}).with_columns(
        pl.lit(None).cast(pl.Date).alias("from_date"),  # Cast None to Date
        pl.lit("A").alias("category"),  # Static category
    )

    # Test grouping with default parameters
    result = group_similar_or_overlapping_range(df, window=["category"])

    expected_df = df.with_columns(
        pl.Series("group_id", [0] * 10)
    ).sort(  # Expected group_id
        ["from_date", "to_date"], descending=[False, True]
    )
    assert result.frame_equal(expected_df)

    # Test grouping with a day override
    result_with_override = group_similar_or_overlapping_range(
        df, window=["category"], day_override=1
    )

    expected_df_with_override = df.sort(
        ["from_date", "to_date"], descending=[False, True]
    ).with_columns(
        pl.Series("group_id", list(range(10)))
    )  # Group ID based on index
    assert result_with_override.frame_equal(expected_df_with_override)
