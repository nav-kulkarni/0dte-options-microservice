import yfinance as yf
import pandas as pd

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

def main():
    ticker_symbol = "AAPL"  # Modify as needed--example stock
    df = fetch_options_data(ticker_symbol)
    print(df.head())  # Display to test

if __name__ == "__main__":
    main() 
