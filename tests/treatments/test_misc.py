from datetime import datetime, timedelta

import pytest
import polars as pl
from faker import Faker

from radar_timeline_data.utils.treatments import split_combined_dataframe

fake = Faker()
date = datetime.now().date()


def test_result_dataframes_empty():
    ids = [fake.uuid4() for _ in range(2)]
    full_dataframe = pl.DataFrame(
        {
            "id": ids,
            "patient_id": [1, 2],
            "modality": [10, 2],
            "from_date": [date + timedelta(3), date + timedelta(4)],
            "to_date": [date + timedelta(12), date + timedelta(8)],
            "created_date": date,
            "modified_date": date,
            "source_group_id": [1, 1],
            "source_type": ["3" for _ in range(2)],
        }
    )
    reduced_dataframe = pl.DataFrame(
        {
            "id": ids[0],
            "patient_id": [1],
            "modality": [10],
            "from_date": [date + timedelta(3)],
            "to_date": [date + timedelta(12)],
            "created_date": date,
            "modified_date": date,
            "source_group_id": [1],
            "source_type": ["RADAR"],
        }
    )

    a, b = split_combined_dataframe(full_dataframe, reduced_dataframe)
    assert a.is_empty()
    assert b.is_empty()


def test_result_dataframes_not_empty():
    ids = [fake.uuid4() for _ in range(2)]
    full_dataframe = pl.DataFrame(
        {
            "id": ids,
            "patient_id": [1, 2],
            "modality": [10, 2],
            "from_date": [date + timedelta(3), date + timedelta(4)],
            "to_date": [date + timedelta(12), date + timedelta(8)],
            "created_date": date,
            "modified_date": date,
            "source_group_id": [1, 1],
            "source_type": ["3" for _ in range(2)],
        }
    )
    reduced_dataframe = pl.DataFrame(
        {
            "id": [ids[0], None],
            "patient_id": [1, 2],
            "modality": [2, 4],
            "from_date": [date + timedelta(3), None],
            "to_date": [date + timedelta(12), None],
            "created_date": [date, date],
            "modified_date": [date, date],
            "source_group_id": [1, 2],
            "source_type": ["RADAR", "RADAR"],
        }
    )
    existing, new = split_combined_dataframe(full_dataframe, reduced_dataframe)

    assert existing.shape[0] == 1
    assert new.shape[0] == 1
    assert new.filter(pl.col("id").is_null()).shape[0] == 1


@pytest.mark.parametrize("total", [100, 1000, 10000])
def test_large_split(total):
    ids = [fake.uuid4() for _ in range(total)]
    full_dataframe = pl.DataFrame(
        {
            "id": ids,
            "patient_id": [i for i in range(total)],
            "modality": [1 for _ in range(total)],
            "from_date": [date + timedelta(i) for i in range(total)],
            "to_date": [date + timedelta(i + 5) for i in range(total)],
            "created_date": date,
            "modified_date": date,
            "source_group_id": [1 for _ in range(total)],
            "source_type": ["3" for _ in range(total)],
        }
    )

    updated_ids = ids[: 1 + len(ids) // 2] + [None for _ in range(len(ids) // 2)]

    reduced_dataframe = pl.DataFrame(
        {
            "id": updated_ids,
            "patient_id": [i for i in range(len(updated_ids))],
            "modality": [i for i in range(len(updated_ids))],
            "from_date": [date + timedelta(i) for i in range(len(updated_ids))],
            "to_date": [date + timedelta(i + 5) for i in range(len(updated_ids))],
            "created_date": date,
            "modified_date": date,
            "source_group_id": 1,
            "source_type": "RADAR",
        }
    )
    existing, new = split_combined_dataframe(full_dataframe, reduced_dataframe)
    assert existing.shape[0] == len(ids) // 2
    assert new.shape[0] == len(ids) // 2
    assert existing.filter(pl.col("id").is_null()).shape[0] == 0
    assert existing.filter(pl.col("id").is_not_null()).shape[0] == len(ids) // 2
