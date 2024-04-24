import polars as pl
from sqlalchemy.orm import Session

from radar_timeline_data.utils.connections import (
    create_sessions,
    SessionManager,
    get_ukrdcid_to_radarnumber_map,
    get_modality_codes,
    sessions_to_treatment_dfs,
    get_sattelite_map,
    get_source_group_id_mapping,
)


def test_get_session():
    """Testing if get_session method returns a SQLAlchemy Session object"""
    session_manager = SessionManager(connection_manger_passthrough="radar_staging")
    session = session_manager.session
    assert isinstance(session, Session)


def test_create_two_sessions():
    """Testing if create_sessions method returns a dictionary containing initialized SessionManager instances"""
    sessions = create_sessions()
    assert isinstance(sessions, dict)
    assert len(sessions) == 3  # Assuming two sessions are created


def test_get_ukrdcid_to_radarnumber_map():
    """Testing if get_ukrdcid_to_radarnumber_map method returns a Polars DataFrame"""
    sessions = create_sessions()
    result = get_ukrdcid_to_radarnumber_map(sessions)
    assert isinstance(result, pl.DataFrame)


def test_sessions_to_treatment_dfs():
    """Testing if sessions_to_treatment_dfs method returns a dictionary containing DataFrames"""
    sessions = create_sessions()
    filter_series = pl.Series([1, 2, 3])
    result = sessions_to_treatment_dfs(sessions, filter_series)
    assert isinstance(result, dict)
    assert len(result) == 2  # Assuming two DataFrames are returned
    for df in result.values():
        assert isinstance(df, pl.DataFrame)


def test_get_modality_codes():
    """Testing if get_modality_codes method returns a Polars DataFrame"""
    sessions = create_sessions()
    result = get_modality_codes(sessions)
    assert isinstance(result, pl.DataFrame)


def test_get_sattelite_map():
    """Testing if get_sattelite_map method returns a Polars DataFrame"""
    sessions = create_sessions()
    result = get_sattelite_map(sessions["ukrdc"])
    assert isinstance(result, pl.DataFrame)


def test_get_source_group_id_mapping():
    """Testing if get_source_group_id_mapping method returns a Polars DataFrame"""
    sessions = create_sessions()
    result = get_source_group_id_mapping(sessions["radar"])
    assert isinstance(result, pl.DataFrame)
    assert ["id", "code"] == result.columns
