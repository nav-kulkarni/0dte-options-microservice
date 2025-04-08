import yfinance as yf
import pandas as pd
import logging
from datetime import datetime
from typing import Optional

def format_timestamp(dt: datetime = None) -> str:
    """Format datetime to string in a consistent format."""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fetch_options_data(ticker_symbol):
    """
    Fetches options data and open interest for a given stock ticker and stores in a DataFrame.
    """
    ticker = yf.Ticker(ticker_symbol)
    options_list = []

    for exp_date in ticker.options:
        opt_chain = ticker.option_chain(exp_date)
        # Add calls
        calls_df = opt_chain.calls[["strike", "openInterest"]].copy()
        calls_df["expiration_date"] = exp_date
        calls_df["type"] = "call"
        
        # Add puts
        puts_df = opt_chain.puts[["strike", "openInterest"]].copy()
        puts_df["expiration_date"] = exp_date
        puts_df["type"] = "put"
        
        options_list.append(calls_df)
        options_list.append(puts_df)

    # Combine into a single DataFrame
    options_data = pd.concat(options_list, ignore_index=True)
    return options_data

def fetch_options_data_v2(ticker_symbol: str) -> Optional[pd.DataFrame]:
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
        
        # Append metadata: a timestamp and the current stock price.
        options_data["timestamp"] = format_timestamp()  # Ensure this helper exists.
        options_data["stock_price"] = current_price
        
        return options_data
        
    except Exception as e:
        logging.exception(f"Error fetching options data for {ticker_symbol}")
        return None

def main():
    ticker_symbol = "SPY"  # Modify as needed--example stock
    df = fetch_options_data_v2(ticker_symbol)
    print(df.head())  # Display to test

if __name__ == "__main__":
    main() 
