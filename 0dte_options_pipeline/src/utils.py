import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


def format_timestamp(dt: datetime = None) -> str:
    """Format datetime to string in a consistent format."""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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

def fetch_options_data(ticker_symbol: str) -> Optional[pd.DataFrame]:
    """
    Fetches options data for a given ticker that is most relevant for same-day trading.
    Preferences:
      - Use options expiring today (0DTE) if available.
      - If not, use the nearest future expiration date.
    Additional metadata (timestamp and current stock price) are added.
    
    Args:
        ticker_symbol (str): Stock ticker to fetch options data for.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing the relevant options data,
                                or None if data collection fails.
    """
    try:
        logging.info(f"Fetching options data for same-day trading for {ticker_symbol}")
        ticker = yf.Ticker(ticker_symbol)
        
        # Retrieve current stock price
        hist = ticker.history(period="1d")
        if hist.empty or "Close" not in hist.columns:
            logging.error(f"No valid closing price data for {ticker_symbol}")
            return None
        current_price = hist["Close"].iloc[-1]
        
        # Determine today's date
        today = datetime.now().date()
        today_str = today.strftime("%Y-%m-%d")
        
        # Ensure options exist
        if not ticker.options:
            logging.error(f"No options available for {ticker_symbol}")
            return None
        
        # Determine the relevant expiration date
        if today_str in ticker.options:
            relevant_exp_date = today_str
            logging.info(f"Using 0DTE options expiring on {today_str}")
        else:
            # Get future expirations (those after today)
            future_expiries = [exp for exp in ticker.options 
                               if datetime.strptime(exp, "%Y-%m-%d").date() > today]
            if not future_expiries:
                logging.error("No future expiration dates found.")
                return None
            # Choose the earliest available expiration date
            relevant_exp_date = min(future_expiries, key=lambda d: datetime.strptime(d, "%Y-%m-%d").date())
            logging.info(f"0DTE not available. Using nearest expiration: {relevant_exp_date}")
        
        # Retrieve option chain data for the chosen expiration date
        opt_chain = ticker.option_chain(relevant_exp_date)
        if opt_chain.calls.empty and opt_chain.puts.empty:
            logging.warning(f"Option chain is empty for expiration date {relevant_exp_date}")
            return None
        
        options_list = []
        # Process calls (check for required columns first)
        if not opt_chain.calls.empty and all(col in opt_chain.calls.columns for col in ["strike", "openInterest"]):
            calls_df = opt_chain.calls[["strike", "openInterest"]].copy()
            calls_df["expiration_date"] = relevant_exp_date
            calls_df["type"] = "call"
            options_list.append(calls_df)
        else:
            logging.warning("Calls data is missing required columns.")
            
        # Process puts similarly
        if not opt_chain.puts.empty and all(col in opt_chain.puts.columns for col in ["strike", "openInterest"]):
            puts_df = opt_chain.puts[["strike", "openInterest"]].copy()
            puts_df["expiration_date"] = relevant_exp_date
            puts_df["type"] = "put"
            options_list.append(puts_df)
        else:
            logging.warning("Puts data is missing required columns.")
        
        if not options_list:
            logging.error(f"No valid options data found for {ticker_symbol}")
            return None
        
        # Combine calls and puts
        options_data = pd.concat(options_list, ignore_index=True)
        
        # Add ticker column
        options_data['ticker'] = ticker_symbol
        
        # Ensure column names match database schema
        options_data = options_data.rename(columns={
            'type': 'option_type',
            'openInterest': 'open_interest'
        })
        
        # Set timestamp
        options_data['timestamp'] = format_timestamp()
        
        # Convert expiration_date to datetime if it's a string
        options_data['expiration_date'] = pd.to_datetime(options_data['expiration_date'])
        
        # Add stock price
        options_data['stock_price'] = current_price
        
        # Validate the data structure
        if not validate_options_data(options_data):
            logging.error(f"Data validation failed for {ticker_symbol}")
            return None
        
        return options_data
        
    except Exception as e:
        logging.exception(f"Error fetching options data for {ticker_symbol}")
        return None


