import pytest
from datetime import datetime, timezone
import pandas as pd
from src.utils import format_timestamp, validate_options_data, fetch_options_data

def test_format_timestamp():
    # Test with current time
    current_time = datetime.now(timezone.utc)
    formatted_time = format_timestamp(current_time)
    assert isinstance(formatted_time, str)
    assert len(formatted_time) > 0

    # Test with specific time
    specific_time = datetime(2024, 3, 29, 11, 30, tzinfo=timezone.utc)
    formatted_specific_time = format_timestamp(specific_time)
    assert isinstance(formatted_specific_time, str)
    assert len(formatted_specific_time) > 0

def test_validate_options_data():
    # Test with valid data: using the updated column names
    valid_data = pd.DataFrame({
        'strike': [100.0, 101.0],
        'open_interest': [500, 600],
        'expiration_date': [datetime.now(), datetime.now()],
        'option_type': ['call', 'put'],
        'stock_price': [150.0, 150.0],
        'timestamp': [datetime.now(), datetime.now()]
    })
    assert validate_options_data(valid_data) is True

    # Test with missing columns (should fail validation)
    invalid_data = pd.DataFrame({
        'strike': [100.0],
        'lastPrice': [1.5]  # invalid column name; required columns not all present
    })
    assert validate_options_data(invalid_data) is False

    # Test with empty dataframe
    empty_data = pd.DataFrame()
    assert validate_options_data(empty_data) is False

def test_fetch_options_data():
    # Test with valid ticker
    data = fetch_options_data('SPY')
    # If using live data, we expect a non-empty DataFrame; otherwise you might want to use mocking.
    assert isinstance(data, pd.DataFrame)
    assert not data.empty

    # Check for expected columns (based on updated utility behavior)
    expected_cols = [
        'ticker',
        'strike',
        'open_interest',
        'expiration_date',
        'option_type',
        'timestamp',
        'stock_price'
    ]
    for col in expected_cols:
        assert col in data.columns

    # Test with an invalid ticker - assuming the utility returns None for invalid tickers
    invalid_data = fetch_options_data('INVALID_TICKER')
    assert invalid_data is None
