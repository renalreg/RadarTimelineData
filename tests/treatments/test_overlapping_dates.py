from datetime import timedelta, datetime

import polars as pl
import pytest
from faker import Faker

from radar_timeline_data.utils.treatments import overlapping_dates_bool_mask

start_date = datetime.strptime("2016-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
fake = Faker()


def create_date_data(
    total,
    from_date_modifier: int | None = 0,
    to_date_modifier: int | None = 5,
    prev_to_modifier: int | None = 5,
    prev_from_modifier: int | None = 5,
):
    """Helper function to create test data for date overlaps."""

    mask = overlapping_dates_bool_mask()

    from_date = pl.Series(
        "from_date",
        [fake.date_between(start_date, end_date) for _ in range(total)],
    )
    to_date = pl.Series(
        "to_date",
        [
            date + timedelta(days=to_date_modifier)
            if to_date_modifier is not None
            else None
            for date in from_date
        ],
    )
    prev_to_date = pl.Series(
        "previous_to_date",
        [
            date + timedelta(days=prev_to_modifier)
            if prev_to_modifier is not None
            else None
            for date in from_date
        ],
    )
    prev_from_date = pl.Series(
        "previous_from_date",
        [
            date + timedelta(days=prev_from_modifier)
            if prev_from_modifier is not None
            else None
            for date in from_date
        ],
    )
    if from_date_modifier is None:
        from_date = pl.Series(
            "from_date",
            [None for _ in range(total)],
        )

    # Create DataFrame
    df = pl.DataFrame(
        {
            "from_date": from_date,
            "to_date": to_date,
            "prev_from_date": prev_from_date,
            "prev_to_date": prev_to_date,
        }
    ).cast(
        {
            "from_date": pl.Date,
            "to_date": pl.Date,
            "prev_from_date": pl.Date,
            "prev_to_date": pl.Date,
        }
    )

    # Apply overlapping mask
    df = df.with_columns(pl.when(mask).then(1).otherwise(0).alias("overlaps"))
    return df


@pytest.mark.parametrize("total", [1000])
def test_simple_overlapping_dates(total):
    """Test overlapping dates condition with dates within 5 days of each other."""
    df = create_date_data(total)
    assert df["overlaps"].sum() == total, "Expected all dates to overlap"


@pytest.mark.parametrize("total", [1000])
def test_non_overlapping_dates(total):
    """Test non-overlapping dates condition with dates well outside of each other's range."""
    df = create_date_data(
        total,
        to_date_modifier=10,
        prev_to_modifier=-50,
        prev_from_modifier=-100,
    )
    assert df["overlaps"].sum() == 0, "Expected no overlaps"


@pytest.mark.parametrize(
    "total, from_date_mod, to_date_mod, prev_to_mod, prev_from_mod, expected_overlap",
    [
        # No overlap cases
        (
            100,
            0,
            10,
            -50,
            -40,
            False,
        ),  # No overlap, both ranges are completely separate
        (
            100,
            0,
            10,
            -10,
            10,
            True,
        ),  # Overlap, current range overlaps with previous range
        # Edge cases with None values
        (100, None, None, -10, 10, False),  # Both current from/to dates are None
        (
            100,
            None,
            10,
            -10,
            10,
            True,
        ),  # from_date is None, but overlap exists due to valid to_date
        (
            100,
            0,
            None,
            -10,
            10,
            False,
        ),  # to_date is None, should not overlap with previous range
        (100, 5, None, -10, 10, False),  # Valid from_date with None to_date, no overlap
        # Complex cases with multiple None values
        (
            100,
            None,
            10,
            None,
            -40,
            False,
        ),  # Both from_date and prev_from_date are None, no overlap
        (
            100,
            0,
            10,
            None,
            None,
            False,
        ),  # Both prev_to_date and prev_from_date are None, no overlap
        (100, None, None, None, None, False),  # All dates are None, no overlap expected
    ],
)
def test_overlap_with_null_dates(
    total,
    from_date_mod,
    to_date_mod,
    prev_to_mod,
    prev_from_mod,
    expected_overlap,
):
    """Test overlap behavior with null and valid dates, dataframe row = a single test"""

    # Create the dataframe with date modifications applied
    df = create_date_data(
        total=total,
        from_date_modifier=from_date_mod,
        to_date_modifier=to_date_mod,
        prev_to_modifier=prev_to_mod,
        prev_from_modifier=prev_from_mod,
    )
    print(df)

    # Check if the number of overlaps matches the expected result
    assert (df["overlaps"].sum() == total) == expected_overlap
