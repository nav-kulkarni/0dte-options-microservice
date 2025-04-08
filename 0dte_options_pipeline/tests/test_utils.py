import pytest
from datetime import datetime, timezone
import pandas as pd
from src.utils import format_timestamp, is_collection_time, validate_options_data, fetch_options_data

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

def test_is_collection_time():
    # Test during collection time (11:30 AM EST)
    collection_time = datetime(2024, 3, 29, 11, 30, tzinfo=timezone.utc)
    assert is_collection_time(collection_time) is True

    # Test outside collection time
    non_collection_time = datetime(2024, 3, 29, 12, 30, tzinfo=timezone.utc)
    assert is_collection_time(non_collection_time) is False

def test_validate_options_data():
    # Test with valid data
    valid_data = pd.DataFrame({
        'strike': [100.0, 101.0],
        'lastPrice': [1.5, 2.0],
        'bid': [1.4, 1.9],
        'ask': [1.6, 2.1],
        'volume': [100, 200],
        'openInterest': [1000, 2000],
        'impliedVolatility': [0.2, 0.25],
        'delta': [0.5, 0.6],
        'gamma': [0.1, 0.15],
        'theta': [-0.05, -0.06],
        'vega': [0.1, 0.12]
    })
    assert validate_options_data(valid_data) is True

    # Test with missing columns
    invalid_data = pd.DataFrame({
        'strike': [100.0],
        'lastPrice': [1.5]
    })
    assert validate_options_data(invalid_data) is False

    # Test with empty dataframe
    empty_data = pd.DataFrame()
    assert validate_options_data(empty_data) is False

def test_fetch_options_data():
    # Test with valid ticker
    data = fetch_options_data('SPY')
    assert isinstance(data, pd.DataFrame)
    assert not data.empty
    assert all(col in data.columns for col in [
        'strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest',
        'impliedVolatility', 'delta', 'gamma', 'theta', 'vega'
    ])

    # Test with invalid ticker
    invalid_data = fetch_options_data('INVALID_TICKER')
    assert invalid_data is None 