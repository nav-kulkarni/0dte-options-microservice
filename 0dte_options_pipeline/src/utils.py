import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Dict, Any
import pytz
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get collection time setting
COLLECTION_TIME = os.getenv('COLLECTION_TIME', '11:30')

def format_timestamp(dt: datetime = None) -> str:
    """Format datetime to string in a consistent format."""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def is_collection_time() -> bool:
    """
    Check if the current time is the collection time (11:30 AM ET).
    
    Returns:
        bool: True if it's collection time, False otherwise
    """
    # Get current time in Eastern Time
    et_tz = pytz.timezone('US/Eastern')
    current_time_et = datetime.now(et_tz)
    
    # Check if it's a weekday
    if current_time_et.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    
    # Parse collection time
    hour, minute = map(int, COLLECTION_TIME.split(':'))
    
    # Create datetime object for collection time
    collection_time = current_time_et.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # Check if current time is within 5 minutes of collection time
    time_diff = abs((current_time_et - collection_time).total_seconds() / 60)
    return time_diff <= 5

def validate_options_data(df: pd.DataFrame) -> bool:
    """Validate the structure and content of options data DataFrame."""
    required_columns = ['strike', 'openInterest', 'expiration_date', 'type', 'stock_price', 'timestamp']
    
    # Check if all required columns exist
    if not all(col in df.columns for col in required_columns):
        logging.error(f"Missing required columns. Expected: {required_columns}, Found: {df.columns.tolist()}")
        return False
    
    # Check for null values in critical columns
    critical_columns = ['strike', 'openInterest', 'type']
    if df[critical_columns].isnull().any().any():
        logging.error("Found null values in critical columns")
        return False
    
    return True

def fetch_0dte_options_data(ticker_symbol: str) -> Optional[pd.DataFrame]:
    """
    Fetch 0DTE options data for a given ticker symbol.
    
    Args:
        ticker_symbol (str): The ticker symbol to fetch options data for
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing 0DTE options data or None if fetch fails
    """
    try:
        # Check if it's collection time
        if not is_collection_time():
            logging.info(f"Not collection time, skipping data fetch for {ticker_symbol}")
            return None
        
        logging.info(f"Fetching 0DTE options data for {ticker_symbol}")
        ticker = yf.Ticker(ticker_symbol)
        
        # Get current stock price
        current_price = ticker.history(period="1d")['Close'].iloc[-1]
        
        # Get today's date
        today = datetime.now().date()
        
        # Get options expiration dates
        if not ticker.options:
            logging.error(f"No options available for {ticker_symbol}")
            return None
        
        # Filter for 0DTE options (expiring today)
        today_str = today.strftime("%Y-%m-%d")
        if today_str not in ticker.options:
            logging.info(f"No 0DTE options available for {ticker_symbol}")
            return None
        
        # Get option chain for today
        opt_chain = ticker.option_chain(today_str)
        
        # Validate option chain data
        if opt_chain.calls.empty and opt_chain.puts.empty:
            logging.warning(f"No options data for expiration date {today_str}")
            return None
        
        options_list = []
        
        # Process calls
        if not opt_chain.calls.empty:
            calls_df = opt_chain.calls[["strike", "openInterest"]].copy()
            calls_df["expiration_date"] = today_str
            calls_df["type"] = "call"
            options_list.append(calls_df)
        
        # Process puts
        if not opt_chain.puts.empty:
            puts_df = opt_chain.puts[["strike", "openInterest"]].copy()
            puts_df["expiration_date"] = today_str
            puts_df["type"] = "put"
            options_list.append(puts_df)
        
        if not options_list:
            logging.error(f"No valid 0DTE options data found for {ticker_symbol}")
            return None
        
        # Combine calls and puts
        options_data = pd.concat(options_list, ignore_index=True)
        
        # Add timestamp and stock price
        options_data['timestamp'] = format_timestamp()
        options_data['stock_price'] = current_price
        
        # Validate data
        if not validate_options_data(options_data):
            logging.error(f"Data validation failed for {ticker_symbol}")
            return None
        
        logging.info(f"Successfully fetched 0DTE options data for {ticker_symbol}")
        return options_data
        
    except Exception as e:
        logging.error(f"Error fetching 0DTE options data for {ticker_symbol}: {str(e)}")
        return None

def calculate_options_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate option Greeks (Delta, Gamma, Theta) for options data.
    
    Args:
        df (pd.DataFrame): DataFrame containing options data
        
    Returns:
        pd.DataFrame: DataFrame with added Greeks columns
    """
    # This is a placeholder for the Greeks calculation
    # In a real implementation, you would use the Black-Scholes model
    # to calculate the Greeks based on the option price, strike, etc.
    
    # For now, we'll just add placeholder columns
    df['delta'] = None
    df['gamma'] = None
    df['theta'] = None
    
    return df

