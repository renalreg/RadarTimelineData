from datetime import date, timedelta

import polars as pl
import pytest
from faker import Faker
from polars.testing import assert_frame_equal

from radar_timeline_data.utils.polarUtil import (
    column_name_and_type_change,
    group_similar_or_overlapping_range,
    overlapping_dates_bool_mask,
    combine_treatment_dataframes,
    fill_null_time,
    split_combined_dataframe,
    group_and_reduce_combined_dataframe,
)


@pytest.fixture
def empty_raw_ukrdc_treatment_dataframe():
    return pl.DataFrame(
        {
            "patient_id": pl.Series([], dtype=pl.String),
            "source_group_id": pl.Series([], dtype=pl.String),
            "creation_date": pl.Series(
                [], dtype=pl.Datetime(time_unit="us", time_zone=None)
            ),
            "update_date": pl.Series(
                [], dtype=pl.Datetime(time_unit="us", time_zone=None)
            ),
            "fromtime": pl.Series(
                [], dtype=pl.Datetime(time_unit="us", time_zone=None)
            ),
            "totime": pl.Series([], dtype=pl.Datetime(time_unit="us", time_zone=None)),
            "admitreasoncode": pl.Series([], dtype=pl.String),
        }
    )


@pytest.fixture
def populated_raw_ukrdc_treatment_dataframe(faker: Faker):
    # Define the number of rows you want to generate
    num_rows = 10

    # Define date series
    date_series_1 = pl.date_range(
        date(2022, 1, 1), date(2022, 1, 5), "1d", eager=True
    ).alias("date")
    date_series_2 = pl.date_range(
        date(2023, 1, 1), date(2023, 1, 5), "1d", eager=True
    ).alias("date")
    to_date_series_1 = pl.date_range(
        date(2022, 1, 5), date(2022, 1, 9), "1d", eager=True
    ).alias("date")
    to_date_series_2 = pl.Series("date", [date(2023, 1, 5), None, None, None, None])

    # Concatenate date series
    date_series = date_series_1.append(date_series_2)
    to_date_series = to_date_series_1.append(to_date_series_2)

    # Generate data using Faker
    data = {
        "patient_id": [0] * num_rows,
        "source_group_id": [faker.random_number() for _ in range(num_rows)],
        "creation_date": [
            faker.date_time_this_year(before_now=True, after_now=False)
            for _ in range(num_rows)
        ],
        "update_date": [
            faker.date_time_this_year(before_now=True, after_now=False)
            for _ in range(num_rows)
        ],
        "fromtime": date_series,
        "totime": to_date_series,
        "admitreasoncode": [faker.word() for _ in range(num_rows)],
    }

    # Convert the generated data to a Polars DataFrame
    df = pl.DataFrame(data)

    return df


@pytest.fixture
def empty_ukrdc_treatment_dataframe():
    return pl.DataFrame(
        {
            "patient_id": pl.Series([], dtype=pl.Int64),
            "source_group_id": pl.Series([], dtype=pl.Int64),
            "created_date": pl.Series(
                [], dtype=pl.Datetime(time_unit="us", time_zone=None)
            ),
            "modified_date": pl.Series(
                [], dtype=pl.Datetime(time_unit="us", time_zone=None)
            ),
            "from_date": pl.Series([], dtype=pl.Date),
            "to_date": pl.Series([], dtype=pl.Date),
            "modality": pl.Series([], dtype=pl.String),
        }
    )


def test_column_name_and_type_change(
    empty_raw_ukrdc_treatment_dataframe, empty_ukrdc_treatment_dataframe
):
    """testing column name and type change function with empty dataframe"""
    df_collection: dict[str, pl.DataFrame] = {
        "ukrdc": empty_raw_ukrdc_treatment_dataframe
    }
    expected_df = empty_ukrdc_treatment_dataframe
    assert_frame_equal(column_name_and_type_change(df_collection)["ukrdc"], expected_df)

    df = {
        "ukrdc": pl.DataFrame(
            {
                "creation_date": [],
                "update_date": [],
                "fromtime": [],
                "totime": [],
                "admitreasoncode": [],
                "source_group_id": [],
                "patient_id": [],
                "dontchange": [],
            }
        )
    }

    df = column_name_and_type_change(df)
    # test column names
    assert df["ukrdc"].columns == [
        "created_date",
        "modified_date",
        "from_date",
        "to_date",
        "modality",
        "source_group_id",
        "patient_id",
        "dontchange",
    ]
    # test data types
    assert df["ukrdc"].dtypes == [
        pl.Null,
        pl.Null,
        pl.Date,
        pl.Date,
        pl.Null,
        pl.Int64,
        pl.Int64,
        pl.Null,
    ]


def test_column_name_and_type_change_with_values(
    populated_raw_ukrdc_treatment_dataframe, empty_ukrdc_treatment_dataframe
):
    """testing column name and type change function with filled dataframe"""
    df_collection: dict[str, pl.DataFrame] = {
        "ukrdc": populated_raw_ukrdc_treatment_dataframe
    }
    expected_df = empty_ukrdc_treatment_dataframe
    result = column_name_and_type_change(df_collection)["ukrdc"]
    assert result.columns == expected_df.columns
    assert result.dtypes == expected_df.dtypes


def test_group_similar_or_overlapping_range(populated_raw_ukrdc_treatment_dataframe):
    """testing grouping overlapping date ranges"""
    df_collection = {"ukrdc": populated_raw_ukrdc_treatment_dataframe}
    result = column_name_and_type_change(df_collection)["ukrdc"]
    groups = group_similar_or_overlapping_range(result, ["patient_id"])
    assert groups.n_unique(subset="group_id") == 2

    # check that error is thrown when invalid dates are given
    result = result.with_columns(pl.lit(None).alias("from_date").cast(pl.Date))
    with pytest.raises(ValueError) as msg:
        group_similar_or_overlapping_range(result, ["patient_id"])
    assert str(msg.value) == "Column 'from_date' contains null values."

    # testing that 2000 rows made of two 1000 overlapping date ranges is split into two groups
    # Number of rows to generate
    x = 1000
    # Generate date ranges for A and B
    from_date = pl.date_range(
        date(2020, 1, 1), date(2021, 12, 31), interval="1d", eager=True
    )
    to_date = pl.date_range(
        date(2022, 1, 1), date(2022, 12, 31), interval="1d", eager=True
    )
    df = pl.DataFrame(
        {
            "patient_id": [1 for _ in range(x)],
            "from_date": from_date.sample(n=x, seed=0, with_replacement=True),
            "to_date": to_date.sample(n=x, seed=0, with_replacement=True),
        }
    ).cast({pl.String: pl.Date})
    from_date = pl.date_range(
        date(2024, 1, 1), date(2024, 12, 31), interval="1d", eager=True
    )
    to_date = pl.date_range(
        date(2025, 1, 1), date(2025, 12, 31), interval="1d", eager=True
    )
    df = pl.concat(
        [
            df,
            pl.DataFrame(
                {
                    "patient_id": [1 for _ in range(x)],
                    "from_date": from_date.sample(n=x, seed=0, with_replacement=True),
                    "to_date": to_date.sample(n=x, seed=0, with_replacement=True),
                }
            ).cast({pl.String: pl.Date}),
        ]
    )
    assert (
        group_similar_or_overlapping_range(df, ["patient_id"])["group_id"].n_unique()
        == 2
    )

    # test for dates not overlapping and not in range of 5 days
    from_date = date(2024, 1, 1)
    df = pl.DataFrame(
        {
            "patient_id": [1 for _ in range(4)],
            "from_date": [from_date + timedelta(days=9 * i) for i in range(4)],
            "to_date": [from_date + timedelta(days=3 + 9 * i) for i in range(4)],
        }
    )

    assert (
        group_similar_or_overlapping_range(df, ["patient_id"])["group_id"].n_unique()
        == 4
    )
    # test for dates not overlapping but within 5 days range
    df = pl.DataFrame(
        {
            "patient_id": [1 for _ in range(4)],
            "from_date": [from_date + timedelta(days=9 * i) for i in range(4)],
            "to_date": [from_date + timedelta(days=4 + 9 * i) for i in range(4)],
        }
    )

    assert (
        group_similar_or_overlapping_range(df, ["patient_id"])["group_id"].n_unique()
        == 1
    )

    # test for dates not overlapping but within 5 days range and different window
    df = pl.DataFrame(
        {
            "patient_id": [1, 2, 1, 2],
            "from_date": [from_date + timedelta(days=9 * i) for i in range(4)],
            "to_date": [from_date + timedelta(days=4 + 9 * i) for i in range(4)],
        }
    )
    assert (
        group_similar_or_overlapping_range(df, ["patient_id"]).n_unique(
            subset=["patient_id", "group_id"]
        )
        == 4
    )


def test_overlapping_dates_bool_mask():
    """testing bool condition for overlapping dates"""
    # Number of rows to generate
    x = 1000
    # Generate date ranges for A and B
    from_date = pl.date_range(
        date(2020, 1, 1), date(2021, 12, 31), interval="1d", eager=True
    )
    to_date = pl.date_range(
        date(2026, 1, 1), date(2027, 12, 31), interval="1d", eager=True
    )
    prev_from_date = pl.date_range(
        date(2018, 1, 1), date(2018, 12, 31), interval="1d", eager=True
    )
    prev_to_date = pl.date_range(
        date(2029, 1, 1), date(2029, 12, 31), interval="1d", eager=True
    )

    df = pl.DataFrame(
        {
            "from_date": from_date.sample(n=x, seed=0, with_replacement=True),
            "to_date": [None for _ in range(x)],
            "prev_from_date": prev_from_date.sample(n=x, seed=0, with_replacement=True),
            "prev_to_date": prev_to_date.sample(n=x, seed=0, with_replacement=True),
        }
    ).cast({pl.String: pl.Date})
    grouped = df.with_columns(
        pl.when(overlapping_dates_bool_mask())
        .then(True)
        .otherwise(False)
        .alias("group_id")
    )
    assert grouped["group_id"].any()

    df = pl.DataFrame(
        {
            "from_date": from_date.sample(n=x, seed=0, with_replacement=True),
            "to_date": to_date.sample(n=x, seed=0, with_replacement=True),
            "prev_from_date": prev_from_date.sample(n=x, seed=0, with_replacement=True),
            "prev_to_date": prev_to_date.sample(n=x, seed=0, with_replacement=True),
        }
    ).cast({pl.String: pl.Date})
    grouped = df.with_columns(
        pl.when(overlapping_dates_bool_mask())
        .then(True)
        .otherwise(False)
        .alias("group_id")
    )
    assert grouped["group_id"].any()

    df = pl.DataFrame(
        {
            "from_date": from_date.sample(n=x, seed=0, with_replacement=True),
            "to_date": to_date.sample(n=x, seed=0, with_replacement=True),
            "prev_from_date": prev_from_date.sample(n=x, seed=0, with_replacement=True),
            "prev_to_date": [None for _ in range(x)],
        }
    ).cast({pl.String: pl.Date})
    grouped = df.with_columns(
        pl.when(overlapping_dates_bool_mask())
        .then(True)
        .otherwise(False)
        .alias("group_id")
    )

    assert grouped["group_id"].any() == False


def test_combine_dataframes():
    """checking that combine dataframes produces correct amount of groups with different dataframe sources"""
    df1 = pl.DataFrame(
        {
            "patient_id": ["group1", "group2"],
            "source_type": ["UKRDC", "UKRDC"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=3),
                date(year=2020, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
        }
    )
    df2 = pl.DataFrame(
        {
            "patient_id": ["group1", "group1"],
            "source_type": ["RADAR", "RADAR"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=12),
                date(year=2021, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=13),
                date(year=2021, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
        }
    )
    df = {"ukrdc": df1, "radar": df2}

    assert (
        combine_treatment_dataframes(df).n_unique(subset=["patient_id", "group_id"])
        == 3
    )


def test_fill_time():
    """check that nulls are filled"""
    df = pl.DataFrame({"modified_date": [None], "created_date": [None]})
    result = fill_null_time(df, df)[0]
    assert (result.null_count().max_horizontal().max()) == 0
    assert result.dtypes == [pl.Datetime, pl.Datetime]


def test_split_combined_dataframe():
    reduced_dataframe = pl.DataFrame(
        {
            "patient_id": ["patient_1", "patient_2"],
            "id": [100, None],
            "from_date": [None, None],
            "to_date": [None, None],
            "modality": [None, None],
            "source_group_id": [None, None],
            "source_type": [None, None],
            "created_user_id": [None, None],
            "created_date": [None, None],
            "modified_user_id": [None, None],
            "modified_date": [None, None],
            "recent_date": [None, None],
        }
    )
    full_dataframe = pl.DataFrame(
        {
            "patient_id": ["patient_1", "patient_2", "none"],
            "id": [100, 200, 400],
            "from_date": [None, None, None],
            "to_date": [None, None, None],
            "modality": [None, None, None],
            "source_group_id": [None, None, None],
            "source_type": [None, None, None],
            "created_user_id": [None, None, None],
            "created_date": [None, None, None],
            "modified_user_id": [None, None, None],
            "modified_date": [None, None, None],
            "recent_date": [None, None, None],
        }
    )

    assert (
        i.shape == (1, 12)
        for i in split_combined_dataframe(full_dataframe, reduced_dataframe)
    )


@pytest.mark.dependency(depends=["test_creation"])
def test_group_reduce():
    """checking that combine dataframes produces correct amount of groups with different dataframe sources"""

    # -----------------------
    # test same date entries but higher priotiy ukrdc<RADAR
    # -----------------------

    df1 = pl.DataFrame(
        {
            "id": [0, 1],
            "patient_id": ["Ben", "Bob"],
            "source_type": ["UKRDC", "UKRDC"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=3),
                date(year=2020, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2024, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )
    df2 = pl.DataFrame(
        {
            "id": [2, 4],
            "patient_id": ["Ben", "Ben"],
            "source_type": ["RADAR", "RADAR"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=12),
                date(year=2021, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=13),
                date(year=2021, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )
    df = {"ukrdc": df1, "radar": df2}
    df = combine_treatment_dataframes(df)
    col = group_and_reduce_combined_dataframe(df).get_column("id").to_list()
    col.sort()
    assert col == [1, 2, 4]

    # -----------------------
    # test same priorty but more recent creation date
    # -----------------------

    df1 = pl.DataFrame(
        {
            "id": [0, 1],
            "patient_id": ["Ben", "Bob"],
            "source_type": ["RADAR", "UKRDC"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=3),
                date(year=2020, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2024, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )

    df = {"ukrdc": df1, "radar": df2}
    df = combine_treatment_dataframes(df)
    col = group_and_reduce_combined_dataframe(df).get_column("id").to_list()
    col.sort()

    assert col == [0, 1, 4]
    # -----------------------
    # test null id replacement
    # -----------------------

    df1 = pl.DataFrame(
        {
            "id": [None, 1],
            "patient_id": ["Ben", "Bob"],
            "source_type": ["RADAR", "UKRDC"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=3),
                date(year=2020, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2024, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )

    df = {"ukrdc": df1, "radar": df2}
    df = combine_treatment_dataframes(df)
    col = group_and_reduce_combined_dataframe(df).get_column("id").to_list()
    col.sort()
    assert col == [1, 2, 4]

    # -----------------------
    # test behaviour with only null ids
    # -----------------------

    df1 = pl.DataFrame(
        {
            "id": [None, None],
            "patient_id": ["Ben", "Bob"],
            "source_type": ["RADAR", "UKRDC"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=3),
                date(year=2020, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2024, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )
    df2 = pl.DataFrame(
        {
            "id": [None, 4],
            "patient_id": ["Ben", "Ben"],
            "source_type": ["RADAR", "RADAR"],
            "modality": ["1", "1"],
            "from_date": [
                date(year=2020, month=4, day=12),
                date(year=2021, month=4, day=2),
            ],
            "to_date": [
                date(year=2020, month=4, day=13),
                date(year=2021, month=4, day=3),
            ],
            "created_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "modified_date": [
                date(year=2020, month=4, day=2),
                date(year=2020, month=4, day=2),
            ],
            "source_group_id": [None, None],
            "created_user_id": [None, None],
            "modified_user_id": [None, None],
        }
    )

    df = {"ukrdc": df1, "radar": df2}
    df = combine_treatment_dataframes(df)
    col = group_and_reduce_combined_dataframe(df).get_column("id").to_list()
    col.sort(key=lambda e: (e is None, e))
    assert col == [4, None, None]
