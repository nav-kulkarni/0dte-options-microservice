import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import yfinance as yf
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from utils import fetch_options_data, format_timestamp

# TODO:
# 1. Set up connections string and test code
# 2. Import fetch_options_data function(copied it for now)
# 3. Test and check logs

load_dotenv(find_dotenv())

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def options_pipeline(myTimer: func.TimerRequest) -> None:
    try:
        if myTimer.past_due:
            logging.warning('The timer is past due!')
        
        logging.info('Starting options pipeline execution')
        
        # Call data collection function, store in pandas frames 
        ticker_symbol = "^GSPC"  
        options_data = fetch_options_data(ticker_symbol)
        
        if options_data is None:
            logging.error("Failed to fetch options data")
            return

        # Get current stock price and time
        try:
            ticker = yf.Ticker(ticker_symbol)
            current_price = ticker.history(period="1d")['Close'].iloc[-1]  # Latest closing price
            current_time = format_timestamp()
            
            # Add stock price and timestamp to the options data
            options_data['stock_price'] = current_price
            options_data['timestamp'] = current_time
        except Exception as e:
            logging.error(f"Error getting current price for {ticker_symbol}: {str(e)}")
            return
        
        # Convert the frame to a csv file
        try:
            csv_buffer = StringIO()
            options_data.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
        except Exception as e:
            logging.error(f"Error converting data to CSV: {str(e)}")
            return
        
        # Azure Blob Storage operations
        try:
            # Get service client via connection string
            blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_BLOB_CONNECTION_STRING"))

            # Container & blob name
            container_name = "options-data" 
            blob_name = f"{ticker_symbol}_options_data_{current_time.replace(' ', '_')}.csv"

            # Check if container exists and if it doesn't create it
            container_client = blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                container_client.create_container()
                logging.info(f"Created container: {container_name}")
            
            # Upload csv buffer 
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(csv_data, overwrite=True)
            
            logging.info(f"Successfully stored options data for {ticker_symbol} in Azure Blob Storage")
            
        except Exception as e:
            logging.error(f"Error with Azure Blob Storage operations: {str(e)}")
            return
            
    except Exception as e:
        logging.error(f"Unexpected error in options pipeline: {str(e)}")
        return