import pytest
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from src.db_operations import (
    get_db_engine,
    initialize_database,
    save_options_data
)

# Load environment variables
load_dotenv()

@pytest.fixture
def sample_options_data():
    """Create sample options data for testing."""
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    
    data = {
        'strike': [100, 105, 110, 100, 105, 110],
        'openInterest': [1000, 2000, 3000, 1500, 2500, 3500],
        'expiration_date': [today, today, today, tomorrow, tomorrow, tomorrow],
        'type': ['call', 'call', 'call', 'put', 'put', 'put'],
        'stock_price': [105, 105, 105, 105, 105, 105],
        'timestamp': [datetime.now()] * 6
    }
    
    return pd.DataFrame(data)

def test_get_db_engine():
    """Test creating a database engine."""
    engine = get_db_engine()
    assert engine is not None

def test_initialize_database():
    """Test initializing the database schema."""
    initialize_database()
    # If no exception is raised, the test passes

def test_save_options_data(sample_options_data):
    """Test saving options data to the database."""
    ticker = "TEST"
    save_options_data(sample_options_data, ticker)
    # If no exception is raised, the test passes 