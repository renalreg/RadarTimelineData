import random

import pytest
import polars as pl

from radar_timeline_data.utils.transplants import format_transplant


@pytest.mark.parametrize("total", [10, 50, 100, 1000])
def test_process_valid_modalities(monkeypatch, total):
    # Define a mock version of `convert_transplant_unit`.
    def mock_convert_transplant_unit(unit, a):
        # Override the return value of `convert_transplant_unit`.
        return unit  # Mocked value, can be anything you want

    # Use monkeypatch to replace the real `convert_transplant_unit` with the mock version.
    monkeypatch.setattr(
        "radar_timeline_data.utils.transplants.convert_transplant_unit",
        mock_convert_transplant_unit,
    )
    df_collection = {}
    sessions = {}
    rr_map = pl.DataFrame(
        {
            "rr_no": [i for i in range(total)],
            "radar_id": [100 + i for i in range(total)],
        }
    )
    choices = [
        "0",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "11",
        "12",
        "15",
        "16",
        "19",
        "10",
    ]

    # Mock rr session DataFrame inside df_collection
    df_collection = {
        "rr": pl.DataFrame(
            {
                "patient_id": [i for i in range(total)],
                "modality": "Live",
                "transplant_relationship": [
                    random.choice(choices) for _ in range(total)
                ],
                "transplant_sex": [random.choice(["1", "2"]) for _ in range(total)],
            }
        )
    }

    before = df_collection["rr"]
    result = format_transplant(df_collection, rr_map, sessions)["rr"]
    assert result.filter(pl.col("modality").is_null()).shape[0] == 0
    assert result.filter(pl.col("modality").is_not_null()).shape[0] == total

    # test for correct father modality
    li = before.filter(
        pl.col("transplant_relationship") == "2", pl.col("transplant_sex") == "1"
    )["patient_id"].to_list()
    li = [i + 100 for i in li]
    assert result.filter(
        pl.col("patient_id").is_in(li), pl.col("modality") == 74
    ).shape[0] == len(li)

    # test for correct mother modality

    li = before.filter(
        pl.col("transplant_relationship") == "2", pl.col("transplant_sex") == "2"
    )["patient_id"].to_list()
    li = [i + 100 for i in li]
    assert result.filter(
        pl.col("patient_id").is_in(li), pl.col("modality") == 75
    ).shape[0] == len(li)

    # test for correct sibling modality

    li = before.filter(
        pl.col("transplant_relationship").is_in(["3", "4", "5", "6", "7", "8"])
    )["patient_id"].to_list()
    li = [i + 100 for i in li]
    assert result.filter(
        pl.col("patient_id").is_in(li), pl.col("modality") == 21
    ).shape[0] == len(li)

    # test for correct sibling modality

    li = before.filter(pl.col("transplant_relationship") == "0")["patient_id"].to_list()
    li = [i + 100 for i in li]
    assert result.filter(
        pl.col("patient_id").is_in(li), pl.col("modality") == 77
    ).shape[0] == len(li)
