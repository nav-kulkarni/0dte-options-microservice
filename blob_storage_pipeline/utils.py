import pandas as pd
import yfinance as yf
from datetime import datetime
import logging
from typing import Optional, List

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
    Fetch options data for a given ticker symbol with error handling and validation.
    
    Args:
        ticker_symbol (str): The ticker symbol to fetch options data for
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing options data or None if fetch fails
    """
    try:
        logging.info(f"Fetching options data for {ticker_symbol}")
        ticker = yf.Ticker(ticker_symbol)
        
        # Verify ticker exists and has options
        if not ticker.options:
            logging.error(f"No options available for {ticker_symbol}")
            return None
        
        options_list: List[pd.DataFrame] = []
        
        for exp_date in ticker.options:
            try:
                opt_chain = ticker.option_chain(exp_date)
                
                # Validate option chain data
                if opt_chain.calls.empty and opt_chain.puts.empty:
                    logging.warning(f"No options data for expiration date {exp_date}")
                    continue
                
                # Process calls
                if not opt_chain.calls.empty:
                    calls_df = opt_chain.calls[["strike", "openInterest"]].copy()
                    calls_df["expiration_date"] = exp_date
                    calls_df["type"] = "call"
                    options_list.append(calls_df)
                
                # Process puts
                if not opt_chain.puts.empty:
                    puts_df = opt_chain.puts[["strike", "openInterest"]].copy()
                    puts_df["expiration_date"] = exp_date
                    puts_df["type"] = "put"
                    options_list.append(puts_df)
                    
            except Exception as e:
                logging.error(f"Error processing expiration date {exp_date}: {str(e)}")
                continue
        
        if not options_list:
            logging.error(f"No valid options data found for {ticker_symbol}")
            return None
        
        options_data = pd.concat(options_list, ignore_index=True)
        
        # Add timestamp and validate data
        options_data['timestamp'] = format_timestamp()
        
        if not validate_options_data(options_data):
            logging.error(f"Data validation failed for {ticker_symbol}")
            return None
        
        logging.info(f"Successfully fetched options data for {ticker_symbol}")
        return options_data
        
    except Exception as e:
        logging.error(f"Error fetching options data for {ticker_symbol}: {str(e)}")
        return None

