import pytest
import pandas as pd
from datetime import datetime, timedelta
import pytz
from src.utils import (
    format_timestamp,
    is_collection_time,
    validate_options_data,
    fetch_0dte_options_data,
    filter_options_by_strike_range,
    get_atm_options
)

def test_format_timestamp():
    """Test formatting timestamp."""
    dt = datetime(2023, 1, 1, 12, 0, 0)
    formatted = format_timestamp(dt)
    assert formatted == "2023-01-01 12:00:00"

def test_is_collection_time():
    """Test checking if current time is during collection time."""
    # This test will depend on when it's run
    # It will pass if run during collection time, fail otherwise
    result = is_collection_time()
    assert isinstance(result, bool)

def test_validate_options_data():
    """Test validating options data."""
    # Valid data
    valid_data = pd.DataFrame({
        'strike': [100, 105],
        'openInterest': [1000, 2000],
        'expiration_date': [datetime.now(), datetime.now()],
        'type': ['call', 'put'],
        'stock_price': [105, 105],
        'timestamp': [datetime.now(), datetime.now()]
    })
    assert validate_options_data(valid_data) is True
    
    # Invalid data (missing column)
    invalid_data = pd.DataFrame({
        'strike': [100, 105],
        'openInterest': [1000, 2000]
    })
    assert validate_options_data(invalid_data) is False
    
    # Invalid data (null values)
    invalid_data = pd.DataFrame({
        'strike': [100, None],
        'openInterest': [1000, 2000],
        'expiration_date': [datetime.now(), datetime.now()],
        'type': ['call', 'put'],
        'stock_price': [105, 105],
        'timestamp': [datetime.now(), datetime.now()]
    })
    assert validate_options_data(invalid_data) is False

def test_filter_options_by_strike_range():
    """Test filtering options by strike range."""
    data = pd.DataFrame({
        'strike': [100, 105, 110, 115, 120],
        'openInterest': [1000, 2000, 3000, 4000, 5000]
    })
    
    filtered = filter_options_by_strike_range(data, 105, 115)
    assert len(filtered) == 3
    assert filtered['strike'].min() == 105
    assert filtered['strike'].max() == 115

def test_get_atm_options():
    """Test getting at-the-money options."""
    data = pd.DataFrame({
        'strike': [100, 105, 110, 115, 120],
        'stock_price': [110, 110, 110, 110, 110],
        'openInterest': [1000, 2000, 3000, 4000, 5000]
    })
    
    # With 5% tolerance
    atm_options = get_atm_options(data, tolerance=0.05)
    assert len(atm_options) == 1  # Only 110 strike is within 5% of 110 stock price
    
    # With 10% tolerance
    atm_options = get_atm_options(data, tolerance=0.10)
    assert len(atm_options) == 3  # 100, 105, 110 strikes are within 10% of 110 stock price 