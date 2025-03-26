import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import yfinance as yf
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv, find_dotenv
from datetime import datetime

# TODO:
# 1. Set up connections string and test code
# 2. Import fetch_options_data function(copied it for now)
# 3. Test and check logs

load_dotenv(find_dotenv())

app = func.FunctionApp()

# Function to fetch and process options data, wrote this in yfinance_data directory
def fetch_options_data(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    options_list = []
    
    for exp_date in ticker.options:
        opt_chain = ticker.option_chain(exp_date)
        
        calls_df = opt_chain.calls[["strike", "openInterest"]].copy()
        calls_df["expiration_date"] = exp_date
        calls_df["type"] = "call"
        
        puts_df = opt_chain.puts[["strike", "openInterest"]].copy()
        puts_df["expiration_date"] = exp_date
        puts_df["type"] = "put"
        
        options_list.append(calls_df)
        options_list.append(puts_df)
    
    options_data = pd.concat(options_list, ignore_index=True)
    return options_data

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def options_pipeline(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Timer trigger properly executed')
    
    # Call data collection function, store in pandas frames 
    ticker_symbol = "^GSPC"  
    options_data = fetch_options_data(ticker_symbol)

    # Get current stock price and time
    ticker = yf.Ticker(ticker_symbol)
    current_price = ticker.history(period="1d")['Close'].iloc[-1]  # Latest closing price
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp

    # Add stock price and timestamp to the options data
    options_data['stock_price'] = current_price
    options_data['timestamp'] = current_time
    
    # Convert convert the frame to a csv file
    csv_buffer = StringIO()
    options_data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    # Get service client via connection string
    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_BLOB_CONNECTION_STRING"))

    # Container & blob name
    container_name = "options-data" 
    blob_name = f"{ticker_symbol}_options_data.csv"

    # Check if container exists and if it doesn's create it
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
    
    # Upload csv buffer 
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    
    logging.info(f"Options data for {ticker_symbol} stored in Azure Blob Storage.")